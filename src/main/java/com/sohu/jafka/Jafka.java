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

import java.util.Properties;

import com.sohu.jafka.consumer.ConsumerConfig;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.server.Config;
import com.sohu.jafka.server.ServerStartable;
import com.sohu.jafka.utils.Utils;

/**
 * Jafka Main point
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
public class Jafka {

    public void start(String mainFile, String consumerFile, String producerFile) {
        start(Utils.loadProps(mainFile),//
                consumerFile == null ? null : Utils.loadProps(consumerFile),//
                producerFile == null ? null : Utils.loadProps(producerFile));
    }

    public void start(Properties mainProperties, Properties consumerProperties, Properties producerProperties) {
        final Config config = new Config(mainProperties);
        final ConsumerConfig consumerConfig = consumerProperties == null ? null : new ConsumerConfig(consumerProperties);
        final ProducerConfig producerConfig = consumerConfig == null ? null : new ProducerConfig(producerProperties);
        start(config, consumerConfig, producerConfig);
    }

    public void start(Config config, ConsumerConfig consumerConfig, ProducerConfig producerConfig) {
        final ServerStartable serverStartable;
        if (consumerConfig == null) {
            serverStartable = new ServerStartable(config);
        } else {
            serverStartable = new ServerStartable(config, consumerConfig, producerConfig);
        }
        //
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                serverStartable.shutdown();
                serverStartable.awaitShutdown();
            }
        });
        //
        serverStartable.startup();
        serverStartable.awaitShutdown();
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
    }
}
