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

package com.sohu.jafka.network;

import static java.lang.String.format;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.sohu.jafka.api.RequestKeys;
import com.sohu.jafka.mx.SocketServerStats;

/**
 * Thread that processes all requests from a single connection. There are N
 * of these running in parallel each of which has its own selectors
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-6
 */
public class Processor extends AbstractServerThread {

    final ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<SocketChannel>();

    final Logger requestLogger = Logger.getLogger("jafka.request.logger");

    HandlerMappingFactory handlerMappingFactory;

    SocketServerStats stats;

    int maxRequestSize;

    public Processor(HandlerMappingFactory handlerMappingFactory,  SocketServerStats stats, int maxRequestSize) {
        this.handlerMappingFactory = handlerMappingFactory;
        this.stats = stats;
        this.maxRequestSize = maxRequestSize;
    }

    public void run() {
        startupComplete();
        while (isRunning()) {
            try {
                // setup any new connections that have been queued up
                configureNewConnections();

                int ready;

                final Selector selector = getSelector();
                ready = selector.select(500);
                if (ready > 0) {
                    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                    while (iter.hasNext() && isRunning()) {
                        SelectionKey key = null;
                        try {
                            key = iter.next();
                            iter.remove();
                            if (key.isReadable()) {
                                read(key);
                            } else if (key.isWritable()) {
                                write(key);
                            } else if (!key.isValid()) {
                                close(key);
                            } else {
                                throw new IllegalStateException("Unrecognized key state for processor thread.");
                            }
                        } catch (EOFException eofe) {
                            Socket socket = channelFor(key).socket();
                            logger.info(format("Closing socket connection to %s:%d.", socket.getInetAddress(),socket.getPort()));
                            close(key);
                        } catch (InvalidRequestException ire) {
                            Socket socket = channelFor(key).socket();
                            logger.info(format("Closing socket connection to %s:%d due to invalid request: %s", socket.getInetAddress(),socket.getPort(),
                                    ire.getMessage()));
                            close(key);
                        } catch (Throwable t) {
                            Socket socket = channelFor(key).socket();
                            logger.error(format("Closing socket for %s:%d becaulse of error" ,socket.getInetAddress(),socket.getPort()), t);
                            close(key);
                        }
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }

        }
        //
        logger.info("Closing selector.");
        closeSelector();
        shutdownComplete();
    }

    private SocketChannel channelFor(SelectionKey key) {
        return (SocketChannel) key.channel();
    }

    /**
     * @param key
     */
    private void close(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        if (logger.isDebugEnabled()) logger.debug("Closing connection from " + channel.socket().getRemoteSocketAddress());
        try {
            channel.socket().close();
        } catch (IOException e) {
            logger.info(e.getMessage(), e);
        } finally {
            try {
                channel.close();
            } catch (IOException e2) {
                logger.info(e2.getMessage(), e2);
            }
        }

        key.attach(null);
        key.cancel();
    }

    /**
     * @param key
     * @throws IOException
     */
    private void write(SelectionKey key) throws IOException {
        Send response = (Send) key.attachment();
        SocketChannel socketChannel = channelFor(key);
        int written = response.writeTo(socketChannel);
        stats.recordBytesWritten(written);
        if (response.complete()) {
            key.attach(null);
            key.interestOps(SelectionKey.OP_READ);
        } else {
            key.interestOps(SelectionKey.OP_WRITE);
            getSelector().wakeup();
        }
    }

    /**
     * @param key
     * @throws IOException
     */
    private void read(SelectionKey key) throws IOException {
        SocketChannel socketChannel = channelFor(key);
        Receive request = null;
        if (key.attachment() == null) {
            request = new BoundedByteBufferReceive(maxRequestSize);
            key.attach(request);
        } else {
            request = (Receive) key.attachment();
        }
        int read = request.readFrom(socketChannel);
        stats.recordBytesRead(read);
        if (read < 0) {
            close(key);
        } else if (request.complete()) {
            Send maybeResponse = handle(key, request);
            key.attach(null);
            // if there is a response, send it, otherwise do nothing
            if (maybeResponse != null) {
                key.attach(maybeResponse);
                key.interestOps(SelectionKey.OP_WRITE);
            }
        } else {
            // more reading to be done
            key.interestOps(SelectionKey.OP_READ);
            getSelector().wakeup();
        }
    }

    /**
     * Handle a completed request producing an optional response
     */
    private Send handle(SelectionKey key, Receive request) {
        final short requestTypeId = request.buffer().getShort();
        final RequestKeys requestType = RequestKeys.valueOf(requestTypeId);
        if (requestLogger.isTraceEnabled()) {
            if (requestType == null) {
                throw new InvalidRequestException("No mapping found for handler id " + requestTypeId);
            }
            String logFormat = "Handling %s request from %s";
            requestLogger.trace(format(logFormat, requestType, channelFor(key).socket().getRemoteSocketAddress()));
        }
        HandlerMapping handlerMapping = handlerMappingFactory.mapping(requestType, request);
        if (handlerMapping == null) {
            throw new InvalidRequestException("No handler found for request");
        }
        long start = System.nanoTime();
        Send maybeSend = handlerMapping.handler(requestType, request);
        stats.recordRequest(requestType,  System.nanoTime() - start);
        return maybeSend;
    }

    private void configureNewConnections() throws ClosedChannelException {
        while (newConnections.size() > 0) {
            SocketChannel channel = newConnections.poll();
            if (logger.isDebugEnabled()) {
                logger.debug("Listening to new connection from " + channel.socket().getRemoteSocketAddress());
            }
            channel.register(getSelector(), SelectionKey.OP_READ);
        }
    }

    public void accept(SocketChannel socketChannel) {
        newConnections.add(socketChannel);
        getSelector().wakeup();
    }

}
