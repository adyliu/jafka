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


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import com.sohu.jafka.utils.Closer;


/**
 * Thread that accepts and configures new connections. There is only need for one of these
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
class Acceptor extends AbstractServerThread {

    private int port;
    private Processor[] processors;
    private int sendBufferSize;
    private int receiveBufferSize;
    
    public Acceptor(int port, Processor[] processors, int sendBufferSize, int receiveBufferSize) {
        super();
        this.port = port;
        this.processors = processors;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
    }

    public void run() {
        final ServerSocketChannel serverChannel;
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(new InetSocketAddress(port));
            serverChannel.register(getSelector(), SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            logger.error("listener on port " + port + " failed.");
            throw new RuntimeException(e);
        }
        //
        logger.debug("Awaiting connection on port "+port);
        startupComplete();
        //
        int currentProcessor = 0;
        while(isRunning()) {
            int ready = -1;
            try {
                ready = getSelector().select(500L);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            if(ready<=0)continue;
            Iterator<SelectionKey> iter = getSelector().selectedKeys().iterator();
            while(iter.hasNext() && isRunning())
                try {
                    SelectionKey key = iter.next();
                    iter.remove();
                    //
                    if(key.isAcceptable()) {
                        accept(key,processors[currentProcessor]);
                    }else {
                        throw new IllegalStateException("Unrecognized key state for acceptor thread.");
                    }
                    //
                    currentProcessor = (currentProcessor + 1) % processors.length;
                } catch (Throwable t) {
                    logger.error("Error in acceptor",t);
                }
            }
        //run over
        logger.info("Closing server socket and selector.");
        Closer.closeQuietly(serverChannel, logger);
        Closer.closeQuietly(getSelector(), logger);
        shutdownComplete();
        }

   
    private void accept(SelectionKey key, Processor processor) throws IOException{
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        serverSocketChannel.socket().setReceiveBufferSize(receiveBufferSize);
        //
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setSendBufferSize(sendBufferSize);
        //
        processor.accept(socketChannel);
    }

}
