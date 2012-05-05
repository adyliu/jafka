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

import org.apache.log4j.Logger;

import com.sohu.jafka.consumer.ConsumerConfig;
import com.sohu.jafka.producer.ProducerConfig;



/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ServerStartable {

    
    private final Logger logger = Logger.getLogger(ServerStartable.class);
    final ServerConfig config;
    final ConsumerConfig consumerConfig;
    final ProducerConfig producerConfig;
    //
    private Server server;
    private EmbeddedConsumer embeddedConsumer;
    public ServerStartable(ServerConfig config, ConsumerConfig consumerConfig, ProducerConfig producerConfig) {
        super();
        this.config = config;
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
        init();
    }
    public ServerStartable(ServerConfig config) {
        this(config,null,null);
    }
    
    private void init() {
        server = new Server(config);
        if(consumerConfig!=null) {
            embeddedConsumer = new EmbeddedConsumer(consumerConfig,producerConfig,this);
        }
    }
    
    public void startup() {
        try {
            server.startup();
            if(embeddedConsumer!=null) {
                embeddedConsumer.startup();
            }
        } catch (Exception e) {
           logger.fatal("Fatal error during ServerStable startup. Prepare to shutdown",e);
           shutdown();
        }
    }
    public void shutdown() {
        try {
            if(embeddedConsumer!=null) {
                embeddedConsumer.shutdown();
            }
            server.shutdown();
        } catch (Exception e) {
            logger.fatal("Fatal error during ServerStable shutdown. Prepare to halt", e);
            Runtime.getRuntime().halt(1);
        }
    }
    
    public void awaitShutdown() {
        try {
            server.awaitShutdown();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage(),e);
        }
    }
}
