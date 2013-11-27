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

package com.sohu.jafka.producer;

import com.sohu.jafka.api.MultiProducerRequest;
import com.sohu.jafka.api.ProducerRequest;
import com.sohu.jafka.common.annotations.ThreadSafe;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.mx.SyncProducerStats;
import com.sohu.jafka.network.BlockingChannel;
import com.sohu.jafka.network.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static java.lang.String.format;

/**
 * file{producer/SyncProducer.scala}
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ThreadSafe
public class SyncProducer implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(SyncProducer.class);

    //private static final RequestKeys RequestKey = RequestKeys.Produce;//0

    /////////////////////////////////////////////////////////////////////
    private final SyncProducerConfig config;

    private final BlockingChannel blockingChannel;

    private final Object lock = new Object();

    private volatile boolean shutdown = false;

    private final String host;

    private final int port;

    public SyncProducer(SyncProducerConfig config) {
        super();
        this.config = config;
        this.host = config.getHost();
        this.port = config.getPort();
        //
        this.blockingChannel = new BlockingChannel(host, port, BlockingChannel.DEFAULT_BUFFER_SIZE, config.bufferSize, config.socketTimeoutMs);
    }

    public void send(String topic, ByteBufferMessageSet message) {
        send(topic, ProducerRequest.RandomPartition, message);
    }

    public void send(String topic, int partition, ByteBufferMessageSet messages) {
        messages.verifyMessageSize(config.maxMessageSize);
        send(new ProducerRequest(topic, partition, messages));
    }

    private void send(Request request) {
        synchronized (lock) {
            long startTime = System.nanoTime();
            int written = -1;
            try {
                written = connect().send(request);
            } catch (IOException e) {
                // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
                disconnect();
                throw new RuntimeException(e);
            } finally {
                if (logger.isDebugEnabled()) {
                    logger.debug(format("write %d bytes data to %s:%d", written, host, port));
                }
            }
            final long endTime = System.nanoTime();
            SyncProducerStats.recordProduceRequest(endTime - startTime);
        }
    }

    private BlockingChannel connect() {
        if (!blockingChannel.isConnected() && !shutdown) {
            try {
                blockingChannel.connect();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe.getMessage(), ioe);
            } finally {
                if (!blockingChannel.isConnected()) {
                    disconnect();
                }
            }
        }
        return blockingChannel;
    }

    private void disconnect() {
        if (blockingChannel.isConnected()) {
            logger.info("Disconnecting from " + config.getHost() + ":" + config.getPort());
            blockingChannel.disconnect();
        }
    }

    public void close() {
        synchronized (lock) {
            try {
                disconnect();
            } finally {
                shutdown = true;
            }
        }
    }


    public void multiSend(List<ProducerRequest> produces) {
        for (ProducerRequest request : produces) {
            request.messages.verifyMessageSize(config.maxMessageSize);
        }
        send(new MultiProducerRequest(produces));
    }

}
