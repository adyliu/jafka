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


import com.sohu.jafka.api.ProducerRequest;
import com.sohu.jafka.common.AsyncProducerInterruptedException;
import com.sohu.jafka.common.QueueClosedException;
import com.sohu.jafka.common.QueueFullException;
import com.sohu.jafka.mx.AsyncProducerQueueSizeStats;
import com.sohu.jafka.mx.AsyncProducerStats;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.SyncProducer;
import com.sohu.jafka.producer.serializer.Encoder;
import com.sohu.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class AsyncProducer<T> implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(AsyncProducer.class);
    private static final Random random = new Random();
    private static final String ProducerQueueSizeMBeanName = "jafka.producer.Producer:type=AsyncProducerQueueSizeStats";
    /////////////////////////////////////////////////////////////////////
    private final SyncProducer producer;


    private final CallbackHandler<T> callbackHandler;
    /////////////////////////////////////////////////////////////////////
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final LinkedBlockingQueue<QueueItem<T>> queue;
    private final int asyncProducerID = AsyncProducer.random.nextInt();
    /////////////////////////////////////////////////////////////////////
    private final ProducerSendThread<T> sendThread;
    private final int enqueueTimeoutMs;

    public AsyncProducer(AsyncProducerConfig config, //
                         SyncProducer producer, //
                         Encoder<T> serializer, //
                         EventHandler<T> eventHandler,//
                         Properties eventHandlerProperties, //
                         CallbackHandler<T> callbackHandler, //
                         Properties callbackHandlerProperties) {
        super();
        this.producer = producer;
        this.callbackHandler = callbackHandler;
        this.enqueueTimeoutMs = config.getEnqueueTimeoutMs();
        //
        this.queue = new LinkedBlockingQueue<QueueItem<T>>(config.getQueueSize());
        //
        if (eventHandler != null) {
            eventHandler.init(eventHandlerProperties);
        }
        if (callbackHandler != null) {
            callbackHandler.init(callbackHandlerProperties);
        }
        this.sendThread = new ProducerSendThread<T>("ProducerSendThread-" + asyncProducerID,
                queue, //
                serializer,//
                producer, //
                eventHandler != null ? eventHandler//
                        : new DefaultEventHandler<T>(new ProducerConfig(config.getProperties()), callbackHandler), //
                callbackHandler, //
                config.getQueueTime(), //
                config.getBatchSize());
        this.sendThread.setDaemon(false);
        AsyncProducerQueueSizeStats<T> stats = new AsyncProducerQueueSizeStats<T>(queue);
        stats.setMbeanName(ProducerQueueSizeMBeanName + "-" + asyncProducerID);
        Utils.registerMBean(stats);
    }

    @SuppressWarnings("unchecked")
    public AsyncProducer(AsyncProducerConfig config) {
        this(config//
                , new SyncProducer(config)//
                , (Encoder<T>) Utils.getObject(config.getSerializerClass())//
                , (EventHandler<T>) Utils.getObject(config.getEventHandler())//
                , config.getEventHandlerProperties()//
                , (CallbackHandler<T>) Utils.getObject(config.getCbkHandler())//
                , config.getCbkHandlerProperties());
    }

    public void start() {
        sendThread.start();
    }

    public void send(String topic, T event) {
        send(topic, event, ProducerRequest.RandomPartition);
    }

    public void send(String topic, T event, int partition) {
        AsyncProducerStats.recordEvent();
        if (closed.get()) {
            throw new QueueClosedException("Attempt to add event to a closed queue.");
        }
        QueueItem<T> data = new QueueItem<T>(event, partition, topic);
        if (this.callbackHandler != null) {
            data = this.callbackHandler.beforeEnqueue(data);
        }

        boolean added = false;
        if (data != null) {
            try {
                if (enqueueTimeoutMs == 0) {
                    added = queue.offer(data);
                } else if (enqueueTimeoutMs < 0) {
                    queue.put(data);
                    added = true;
                } else {
                    added = queue.offer(data, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                throw new AsyncProducerInterruptedException(e);
            }
        }
        if (this.callbackHandler != null) {
            this.callbackHandler.afterEnqueue(data, added);
        }

        if (!added) {
            AsyncProducerStats.recordDroppedEvents();
            throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + event);
        }

    }

    public void close() {
        if (this.callbackHandler != null) {
            callbackHandler.close();
        }
        closed.set(true);
        sendThread.shutdown();
        sendThread.awaitShutdown();
        producer.close();
        logger.info("Closed AsyncProducer");
    }
}
