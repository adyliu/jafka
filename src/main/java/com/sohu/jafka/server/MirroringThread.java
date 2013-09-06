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

package com.sohu.jafka.server;

import com.sohu.jafka.consumer.MessageStream;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class MirroringThread extends Thread implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(MirroringThread.class);

    private final MessageStream<Message> stream;

    private final String topic;

    private final int threadId;

    //
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    private final Producer<Void, Message> producer;

    public MirroringThread(MessageStream<Message> stream, String topic, int threadId, Producer<Void, Message> producer) {
        super();
        this.stream = stream;
        this.topic = topic;
        this.threadId = threadId;
        this.producer = producer;
        //
        this.setDaemon(false);
        this.setName(String.format("jafka-embedded-consumer-%s-%d", topic, threadId));
    }

    @Override
    public void run() {
        logger.info(String.format("Starting mirroring thread %s for topic %s and stream %d", getName(), topic, threadId));

        try {
            for (Message message : stream) {
                ProducerData<Void, Message> pd = new ProducerData<Void, Message>(topic, message);
                producer.send(pd);
            }
        } catch (Exception e) {
            logger.error(topic + " stream " + threadId + " unexpectedly existed", e);

        } finally {
            shutdownComplete.countDown();
            logger.info("Stopped mirroring thread " + getName() + " for topic " + topic + " and stream " + threadId);
        }
    }

    public void close() {
        try {
            shutdownComplete.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Shutdown of thread " + getName() + " interrupted.  Mirroring thread might leak data!");
        }
    }

}
