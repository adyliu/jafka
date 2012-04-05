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

import org.apache.log4j.Logger;

import com.sohu.jafka.mx.SocketServerStats;
import com.sohu.jafka.server.Server;
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
public class SocketServer {

    private final Logger logger = Logger.getLogger(Server.class);

    private final HandlerMappingFactory handlerFactory;

    private final int maxRequestSize;

    //
    private final Processor[] processors;

    private final Acceptor acceptor;

    private final SocketServerStats stats;

    public SocketServer(int port, //
            int numProcessorThreads,//
            int monitoringPeriodSecs, //
            HandlerMappingFactory handlerFactory, //
            int sendBufferSize,//
            int receiveBufferSize, //
            int maxRequestSize) {
        super();
        this.handlerFactory = handlerFactory;
        this.maxRequestSize = maxRequestSize;
        this.processors = new Processor[numProcessorThreads];
        this.stats = new SocketServerStats(1000L * 1000L * 1000L * monitoringPeriodSecs);
        this.acceptor = new Acceptor(port, processors, sendBufferSize, receiveBufferSize);
    }

    /**
     * Shutdown the socket server
     */
    public void shutdown() throws InterruptedException {
        acceptor.shutdown();
        for (Processor processor : processors) {
            processor.shutdown();
        }
    }

    /**
     * Start the socket server
     * 
     * @throws InterruptedException
     */
    public void startup() throws InterruptedException {
        logger.info("start " + processors.length + " Processor threads");
        for (int i = 0; i < processors.length; i++) {
            processors[i] = new Processor(handlerFactory, stats, maxRequestSize);
            Utils.newThread("jafka-processor-" + i, processors[i], false).start();
        }
        Utils.newThread("jafka-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
    }

    /**
     * @return the stats
     */
    public SocketServerStats getStats() {
        return stats;
    }
}
