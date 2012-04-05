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


import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.sohu.jafka.api.MultiProducerRequest;
import com.sohu.jafka.api.ProducerRequest;
import com.sohu.jafka.common.annotations.ThreadSafe;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.mx.SyncProducerStats;
import com.sohu.jafka.network.BoundedByteBufferSend;
import com.sohu.jafka.utils.Closer;
import com.sohu.jafka.utils.Time;

/**
 * file{producer/SyncProducer.scala}
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
@ThreadSafe
public class SyncProducer implements Closeable {

    private final Logger logger = Logger.getLogger(SyncProducer.class);

    //private static final RequestKeys RequestKey = RequestKeys.Produce;//0
    private static final Random randomGenerator = new Random();

    /////////////////////////////////////////////////////////////////////
    private final SyncProducerConfig config;

    private final int MaxConnectBackoffMs = 60000;

    private SocketChannel channel = null;

    private int sentOnConnection = 0;

    private long lastConnectionTime;

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
        lastConnectionTime = System.currentTimeMillis() - (long) (randomGenerator.nextDouble() * config.reconnectInterval);
    }

    public void send(String topic, ByteBufferMessageSet message) {
        send(topic, ProducerRequest.RandomPartition, message);
    }

    /**
     * send a message
     * 
     * @param topic
     * @param partition
     * @param messages
     */
    public void send(String topic, int partition, ByteBufferMessageSet messages) {
        messages.verifyMessageSize(config.maxMessageSize);
        send(new BoundedByteBufferSend(new ProducerRequest(topic, partition, messages)));
    }

    private void send(BoundedByteBufferSend send) {
        synchronized (lock) {
            verifySendBuffer(send.getBuffer().slice());
            long startTime = Time.SystemTime.nanoseconds();
            getOrMakeConnection();
            if (logger.isDebugEnabled()) {
                logger.debug("write data to " + host + ":" + port);
            }
            try {
                send.writeCompletely(channel);
            } catch (IOException e) {
                // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
                disconnect();
                throw new RuntimeException(e);
            }
            sentOnConnection++;
            if (sentOnConnection >= config.reconnectInterval//
                    || (config.reconnectTimeInterval >= 0 && System.currentTimeMillis() - lastConnectionTime >= config.reconnectTimeInterval)) {
                disconnect();
                channel = connect();
                sentOnConnection = 0;
                lastConnectionTime = System.currentTimeMillis();
            }
            final long endTime = Time.SystemTime.nanoseconds();
            SyncProducerStats.recordProduceRequest(endTime - startTime);
        }
    }

    private void getOrMakeConnection() {
        if (channel == null) {
            channel = connect();
        }
    }

    private SocketChannel connect() {
        long connectBackoffMs = 1;
        long beginTimeMs = Time.SystemTime.milliseconds();
        while (channel == null && !shutdown) {
            try {
                channel = SocketChannel.open();
                channel.socket().setSendBufferSize(config.bufferSize);
                channel.configureBlocking(true);
                channel.socket().setSoTimeout(config.getSocketTimeoutMs());
                channel.socket().setKeepAlive(true);
                channel.connect(new InetSocketAddress(config.getHost(), config.getPort()));
                logger.info("Connected to " + config.getHost() + ":" + config.getPort() + " for producing");
            } catch (IOException e) {
                disconnect();
                long endTimeMs = Time.SystemTime.milliseconds();
                if ((endTimeMs - beginTimeMs + connectBackoffMs) > config.connectTimeoutMs) {
                    logger.error(
                            "Producer connection to " + config.getHost() + ":" + config.getPort() + " timing out after " + config.connectTimeoutMs + " ms", e);
                    throw new RuntimeException(e.getMessage(), e);
                }
                logger.error("Connection attempt to " + config.getHost() + ":" + config.getPort() + " failed, next attempt in " + connectBackoffMs + " ms", e);
                try {
                    Time.SystemTime.sleep(connectBackoffMs);
                } catch (InterruptedException e1) {
                    logger.warn(e1.getMessage());
                    Thread.currentThread().interrupt();
                }
                connectBackoffMs = Math.min(10 * connectBackoffMs, MaxConnectBackoffMs);
            }
        }
        return channel;
    }

    private void disconnect() {
        if (channel != null) {
            logger.info("Disconnecting from " + config.getHost() + ":" + config.getPort());
            Closer.closeQuietly(channel);
            Closer.closeQuietly(channel.socket());
            channel = null;
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

    /**
     * @param slice
     */
    private void verifySendBuffer(ByteBuffer slice) {
        //TODO: check the source
    }

    public void multiSend(List<ProducerRequest> produces) {
        for (ProducerRequest request : produces) {
            request.getMessages().verifyMessageSize(config.maxMessageSize);
        }
        send(new BoundedByteBufferSend(new MultiProducerRequest(produces)));
    }

}
