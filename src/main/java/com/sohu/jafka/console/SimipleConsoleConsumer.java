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

package com.sohu.jafka.console;

import java.io.IOException;
import java.net.URI;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.consumer.SimpleConsumer;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.utils.Closer;
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SimipleConsoleConsumer {


    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> topicIdOpt = parser.accepts("topic", "REQUIRED: The topic id to consumer on.")//
                .withRequiredArg().describedAs("topic").ofType(String.class);
        ArgumentAcceptingOptionSpec<String> serverOpt = parser.accepts("server", "REQUIRED: The jafka server connection string.")//
                .withRequiredArg().describedAs("jafka://hostname:port").ofType(String.class);
        ArgumentAcceptingOptionSpec<Long> offsetOpt = parser.accepts("offset", "The offset to start consuming from.")//
                .withRequiredArg().describedAs("offset").ofType(Long.class).defaultsTo(0L);

        //
        OptionSet options = parser.parse(args);
        checkRequiredArgs(parser, options, topicIdOpt, serverOpt);

        final URI server = new URI(options.valueOf(serverOpt));
        final String topic = options.valueOf(topicIdOpt);
        final long startingOffset = options.valueOf(offsetOpt).longValue();
        //
        final SimpleConsumer consumer = new SimpleConsumer(server.getHost(), server.getPort(), 10000, 64 * 1024);
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                Closer.closeQuietly(consumer);
            }
        });
        //
        Thread thread = new Thread() {

            public void run() {
                long offset = startingOffset;
                int consumed = 0;
                while (true) {
                    try {
                        FetchRequest fetchRequest = new FetchRequest(topic, 0, offset, 1000000);
                        ByteBufferMessageSet messageSets = consumer.fetch(fetchRequest);
                        boolean empty = true;
                        for (MessageAndOffset messageAndOffset : messageSets) {
                            empty = false;
                            consumed++;
                            offset = messageAndOffset.offset;
                            System.out.println(String.format("[%d] %d: %s", consumed, offset, //
                                    Utils.toString(messageAndOffset.message.payload(), "UTF-8")));
                        }
                        if(empty) {
                            Thread.sleep(1000L);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        };
        thread.start();
        thread.join();
    }

    static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec<?>... optionSepcs) throws IOException {
        for (OptionSpec<?> arg : optionSepcs) {
            if (!options.has(arg)) {
                System.err.println("Missing required argument " + arg);
                // parser.formatHelpWith(new MyFormatter());
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }
    }
}
