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

package com.sohu.jafka;

import com.sohu.jafka.consumer.ConsumerConfig;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.server.ServerConfig;
import com.sohu.jafka.server.ServerStartable;
import com.sohu.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.Properties;

/**
 * Jafka Main point
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Jafka implements Closeable {

    private volatile Thread shutdownHook;

    //for test privacy
    private ServerStartable serverStartable;
    // server listening port
    private int port = -1;
    private final Logger logger = LoggerFactory.getLogger(Jafka.class);

    public void start(String mainFileName, String consumerFile, String producerFile) {
        File mainFile = Utils.getCanonicalFile(new File(mainFileName));
        if (!mainFile.isFile() || !mainFile.exists()) {
            System.err.println(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", mainFile.getAbsolutePath()));
            System.exit(2);
        }
        start(Utils.loadProps(mainFileName),//
                consumerFile == null ? null : Utils.loadProps(consumerFile),//
                producerFile == null ? null : Utils.loadProps(producerFile));
    }

    public void start(Properties mainProperties, Properties consumerProperties, Properties producerProperties) {
        final ServerConfig config = new ServerConfig(mainProperties);
        final ConsumerConfig consumerConfig = consumerProperties == null ? null : new ConsumerConfig(consumerProperties);
        final ProducerConfig producerConfig = consumerConfig == null ? null : new ProducerConfig(producerProperties);
        start(config, consumerConfig, producerConfig);
    }

    public void start(ServerConfig config, ConsumerConfig consumerConfig, ProducerConfig producerConfig) {
        if (consumerConfig == null) {
            serverStartable = new ServerStartable(config);
        } else {
            serverStartable = new ServerStartable(config, consumerConfig, producerConfig);
        }
        //
        shutdownHook = new Thread() {

            @Override
            public void run() {
                serverStartable.close();
                serverStartable.awaitShutdown();
            }
        };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        //
        serverStartable.startup();
        port = config.getPort();
    }

    public void awaitShutdown() {
        if (serverStartable != null) {
            serverStartable.awaitShutdown();
        }
    }

    @Override
    public synchronized void close() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ex) {
                //ignore shutting down status
            }
            shutdownHook.run();
            shutdownHook = null;
            port = -1;
        }
    }

    /**
     * get the server listening port
     *
     * @return listening port
     */
    public int getPort() {
        return this.port;
    }

    /**
     * flush all messages to disk(this method is used for test)
     */
    void flush() {
        if (serverStartable != null) {
            serverStartable.flush();
        }
    }

    public static void main(String[] args) {
        int argsSize = args.length;
        if (argsSize != 1 && argsSize != 3) {
            System.out.println("USAGE: java [options] Jafka server.properties [consumer.properties producer.properties]");
            System.exit(1);
        }
        //
        Jafka jafka = new Jafka();
        jafka.start(args[0], argsSize > 1 ? args[1] : null, argsSize > 1 ? args[2] : null);
        jafka.awaitShutdown();
        jafka.close();
    }
}
