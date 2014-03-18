/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sohu.jafka.producer.async;

import com.sohu.jafka.common.IllegalQueueStateException;
import com.sohu.jafka.producer.SyncProducer;
import com.sohu.jafka.producer.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ProducerSendThread<T> extends Thread {

    final String threadName;

    final BlockingQueue<QueueItem<T>> queue;

    final Encoder<T> serializer;

    final SyncProducer underlyingProducer;

    final EventHandler<T> eventHandler;

    final CallbackHandler<T> callbackHandler;

    final long queueTime;

    final int batchSize;

    private final Logger logger = LoggerFactory.getLogger(ProducerSendThread.class);

    /////////////////////////////////////////////////////////////////////
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private volatile boolean shutdown = false;

    public ProducerSendThread(String threadName, //
                              BlockingQueue<QueueItem<T>> queue, //
                              Encoder<T> serializer, //
                              SyncProducer underlyingProducer,//
                              EventHandler<T> eventHandler, //
                              CallbackHandler<T> callbackHandler, //
                              long queueTime, //
                              int batchSize) {
        super();
        this.threadName = threadName;
        this.queue = queue;
        this.serializer = serializer;
        this.underlyingProducer = underlyingProducer;
        this.eventHandler = eventHandler;
        this.callbackHandler = callbackHandler;
        this.queueTime = queueTime;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            List<QueueItem<T>> remainingEvents = processEvents();
            //handle remaining events
            if (remainingEvents.size() > 0) {
                logger.debug(format("Dispatching last batch of %d events to the event handler", remainingEvents.size()));
                tryToHandle(remainingEvents);
            }
        } catch (Exception e) {
            logger.error("Error in sending events: ", e);
        } finally {
            shutdownLatch.countDown();
        }
    }

    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
    }

    public void shutdown() {
        shutdown = true;
        eventHandler.close();
        logger.info("Shutdown thread complete");
    }

    private List<QueueItem<T>> processEvents() {
        long lastSend = System.currentTimeMillis();
        final List<QueueItem<T>> events = new ArrayList<QueueItem<T>>();
        boolean full = false;
        while (!shutdown) {
            try {
                QueueItem<T> item = queue.poll(Math.max(0, (lastSend + queueTime) - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                long elapsed = System.currentTimeMillis() - lastSend;
                boolean expired = item == null;
                if (item != null) {
                    if (callbackHandler != null) {
                        List<QueueItem<T>> items = callbackHandler.afterDequeuingExistingData(item);
                        if (items != null) {
                            events.addAll(items);
                        }
                    } else {
                        events.add(item);
                    }
                    //
                    full = events.size() >= batchSize;
                }
                if (full || expired) {
                    if (logger.isDebugEnabled()) {
                        if (expired) {
                            logger.debug(elapsed + " ms elapsed. Queue time reached. Sending..");
                        } else {
                            logger.debug(format("Batch(%d) full. Sending..", batchSize));
                        }
                    }
                    tryToHandle(events);
                    lastSend = System.currentTimeMillis();
                    events.clear();
                }
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
        if (queue.size() > 0) {
            throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, " + queue.size() + " remaining items in the queue");
        }
        if (this.callbackHandler != null) {
            List<QueueItem<T>> remainEvents = this.callbackHandler.lastBatchBeforeClose();
            if (remainEvents != null) {
                events.addAll(remainEvents);
            }
        }
        return events;
    }

    private void tryToHandle(List<QueueItem<T>> events) {
        if (logger.isDebugEnabled()) {
            logger.debug("handling " + events.size() + " events");
        }
        if (events.size() > 0) {
            try {
                this.eventHandler.handle(events, underlyingProducer, serializer);
            } catch (RuntimeException e) {
                logger.error("Error in handling batch of " + events.size() + " events", e);
            }
        }
    }
}
