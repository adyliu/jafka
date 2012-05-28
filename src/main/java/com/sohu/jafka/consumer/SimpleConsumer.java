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

package com.sohu.jafka.consumer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sohu.jafka.api.CreaterRequest;
import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.api.MultiFetchRequest;
import com.sohu.jafka.api.MultiFetchResponse;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.common.annotations.ClientSide;
import com.sohu.jafka.common.annotations.ThreadSafe;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.network.BoundedByteBufferReceive;
import com.sohu.jafka.network.BoundedByteBufferSend;
import com.sohu.jafka.network.Receive;
import com.sohu.jafka.network.Request;
import com.sohu.jafka.utils.Closer;
import com.sohu.jafka.utils.KV;
import com.sohu.jafka.utils.Utils;

/**
 * Simple message consumer
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ThreadSafe
@ClientSide
public class SimpleConsumer implements IConsumer {

    private final Logger logger = Logger.getLogger(SimpleConsumer.class);

    private final String host;

    private final int port;

    private final int soTimeout;

    private final int bufferSize;

    ////////////////////////////////
    private SocketChannel channel = null;

    private final Object lock = new Object();

    public SimpleConsumer(String host, int port) {
        this(host, port, 30 * 1000, 64 * 1024);
    }

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize) {
        super();
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
    }

    private SocketChannel connect() throws IOException {
        close();
        InetSocketAddress address = new InetSocketAddress(host, port);
        SocketChannel ch = SocketChannel.open();
        logger.debug("Connected to " + address + " for fetching");
        ch.configureBlocking(true);
        ch.socket().setReceiveBufferSize(bufferSize);
        ch.socket().setSoTimeout(soTimeout);
        ch.socket().setKeepAlive(true);
        ch.socket().setTcpNoDelay(true);
        ch.connect(address);
        return ch;
    }

    public void close() {
        synchronized (lock) {
            if (channel != null) {
                close(channel);
                channel = null;
            }
        }
    }

    private void close(SocketChannel socketChannel) {
        logger.debug("Disconnecting consumer from " + channel.socket().getRemoteSocketAddress());
        Closer.closeQuietly(socketChannel);
        Closer.closeQuietly(socketChannel.socket());
    }

    private void getOrMakeConnection() throws IOException {
        if (channel == null) {
            channel = connect();
        }
    }

    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
        synchronized (lock) {
            getOrMakeConnection();
            KV<Receive, ErrorMapping> response;

            try {
                sendRequest(request);
                response = getResponse();
            } catch (IOException e) {
                logger.info("Reconnect in fetch request due to socket error:", e);
                //retry once
                try {
                    channel = connect();
                    sendRequest(request);
                    response = getResponse();
                } catch (IOException e2) {
                    throw e2;
                }
            }
            //
            return new ByteBufferMessageSet(response.k.buffer(), request.offset, response.v);
        }
    }

    private void sendRequest(Request request) throws IOException {
        new BoundedByteBufferSend(request).writeCompletely(channel);
    }

    private KV<Receive, ErrorMapping> getResponse() throws IOException {
        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(channel);
        return new KV<Receive, ErrorMapping>(response, ErrorMapping.valueOf(response.buffer().getShort()));
    }

    public int createPartitions(String topic,int partitionNum,boolean enlarge) throws IOException{
        synchronized (lock) {
            KV<Receive, ErrorMapping> response = null;
            try {
                getOrMakeConnection();
                sendRequest(new CreaterRequest(topic, partitionNum, enlarge));
                response = getResponse();
            } catch (IOException e) {
                logger.info("Reconnect in createPartitions request due to socket error: ", e);
                //retry once
                try {
                    channel = connect();
                    sendRequest(new CreaterRequest(topic, partitionNum, enlarge));
                    response = getResponse();
                } catch (IOException e2) {
                    channel = null;
                    throw e2;
                }
            }
            return Utils.deserializeIntArray(response.k.buffer())[0];
        }
    }
    
    public long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) throws IOException {
        synchronized (lock) {
            KV<Receive, ErrorMapping> response = null;
            try {
                getOrMakeConnection();
                sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets));
                response = getResponse();
            } catch (IOException e) {
                logger.info("Reconnect in get offset request due to socket error: ", e);
                //retry once
                try {
                    channel = connect();
                    sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets));
                    response = getResponse();
                } catch (IOException e2) {
                    channel = null;
                    throw e2;
                }
            }
            return OffsetRequest.deserializeOffsetArray(response.k.buffer());
        }
    }

    public MultiFetchResponse multifetch(List<FetchRequest> fetches) throws IOException {
        synchronized (lock) {
            getOrMakeConnection();
            KV<Receive, ErrorMapping> response = null;
            try {
                sendRequest(new MultiFetchRequest(fetches));
                response = getResponse();
            } catch (IOException ioe) {
                logger.info("Reconnect in multifetch due to socket error: ", ioe);
                //retry once
                try {
                    channel = connect();
                    sendRequest(new MultiFetchRequest(fetches));
                    response = getResponse();
                } catch (IOException ioe2) {
                    channel = null;
                    throw ioe2;
                }
            }
            List<Long> offsets = new ArrayList<Long>();
            for (FetchRequest fetch : fetches) {
                offsets.add(fetch.offset);
            }
            return new MultiFetchResponse(response.k.buffer(), fetches.size(), offsets);
        }
    }
    
    @Override
    public long getLatestOffset(String topic, int partition) throws IOException {
        long[] result = getOffsetsBefore(topic, partition, -1, 1);
        return result.length == 0 ? -1 : result[0];
    }
}
