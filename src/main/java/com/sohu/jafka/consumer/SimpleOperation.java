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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;


import com.sohu.jafka.api.FetchRequest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple operation with jafka broker
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
@ThreadSafe
@ClientSide
public class SimpleOperation implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(SimpleOperation.class);

    private final String host;

    private final int port;

    private final int soTimeout;

    private final int bufferSize;

    ////////////////////////////////
    private SocketChannel channel = null;

    private final Object lock = new Object();

    public SimpleOperation(String host, int port) {
        this(host, port, 30 * 1000, 64 * 1024);
    }

    public SimpleOperation(String host, int port, int soTimeout, int bufferSize) {
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

    public static interface Command<T> {

        T run() throws IOException;
    }

    private class SimpleCommand implements Command<KV<Receive, ErrorMapping>> {

        private Request request;

        public SimpleCommand(Request request) {
            this.request = request;
        }

        public KV<Receive, ErrorMapping> run() throws IOException {
            synchronized (lock) {
                getOrMakeConnection();
                try {
                    sendRequest(request);
                    return getResponse();
                } catch (IOException e) {
                    logger.info("Reconnect in fetch request due to socket error:", e);
                    //retry once
                    try {
                        channel = connect();
                        sendRequest(request);
                        return getResponse();
                    } catch (IOException e2) {
                        throw e2;
                    }
                }
                //
            }
        }
    }

    public KV<Receive, ErrorMapping> send(Request request) throws IOException {
        return new SimpleCommand(request).run();
    }

    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
        KV<Receive, ErrorMapping> response = send(request);
        return new ByteBufferMessageSet(response.k.buffer(), request.offset, response.v);
    }

    protected void sendRequest(Request request) throws IOException {
        new BoundedByteBufferSend(request).writeCompletely(channel);
    }

    protected KV<Receive, ErrorMapping> getResponse() throws IOException {
        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(channel);
        return new KV<Receive, ErrorMapping>(response, ErrorMapping.valueOf(response.buffer().getShort()));
    }

}
