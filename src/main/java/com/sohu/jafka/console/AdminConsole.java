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

import static java.lang.String.format;

import java.io.IOException;
import java.util.Arrays;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import com.sohu.jafka.admin.AdminOperation;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
public class AdminConsole {

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> hostOpt = parser.acceptsAll(Arrays.asList("h", "host"), "server address")//
                .withRequiredArg().describedAs("host").ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> portOpt = parser.acceptsAll(Arrays.asList("p", "port"), "server port")//
                .withRequiredArg().describedAs("port").ofType(int.class);
        ArgumentAcceptingOptionSpec<String> topicOpt = parser.acceptsAll(Arrays.asList("t", "topic"), "topic name")//
                .withRequiredArg().describedAs("topic").ofType(String.class);
        //
        parser.acceptsAll(Arrays.asList("c", "create"), "create topic");
        parser.acceptsAll(Arrays.asList("d", "delete"), "delete topic");
        //
        ArgumentAcceptingOptionSpec<Integer> partitionOpt = parser
                .acceptsAll(Arrays.asList("P", "partition"), "topic partition")//
                .withOptionalArg().describedAs("partition").ofType(int.class).defaultsTo(1);
        parser.acceptsAll(Arrays.asList("e", "enlarge"), "enlarge partition number if exists");
        //
        ArgumentAcceptingOptionSpec<String> passwordOpt = parser
                .acceptsAll(Arrays.asList("password"), "jafka password")//
                .withOptionalArg().describedAs("password").ofType(String.class);
        //
        OptionSet options = parser.parse(args);
        boolean create = options.has("c") || options.has("create");
        boolean delete = options.has("d") || options.has("delete");
        if (create && delete || !(create || delete)) {
            printHelp(parser, null);
        }
        checkRequiredArgs(parser, options, hostOpt, portOpt, topicOpt);

        String topic = options.valueOf(topicOpt);
        String host = options.valueOf(hostOpt);
        int port = options.valueOf(portOpt);

        AdminOperation admin = new AdminOperation(host, port);
        try {
            if (create) {
                int partitionNum = options.valueOf(partitionOpt);
                boolean enlarge = options.has("e") || options.has("enlarge");
                int result = admin.createPartitions(topic, partitionNum, enlarge);
                System.out.println(format("create %d partitions for topic [%s]", result, topic));
            } else {
                final String password = options.valueOf(passwordOpt);
                int result = admin.deleteTopic(topic, password);
                System.out.println(format("delete %d partitions for topic [%s]", result, topic));
            }
        } finally {
            admin.close();
        }
    }

    static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec<?>... optionSepcs)
            throws IOException {
        for (OptionSpec<?> arg : optionSepcs) {
            if (!options.has(arg)) {
                printHelp(parser, arg);
            }
        }
    }

    private static void printHelp(OptionParser parser, OptionSpec<?> arg) throws IOException {
        System.err.println("Create/Delete Topic");
        System.err.println("Usage: -c -h <host> -p <port> -t <topic> -P <partition> [-e]");
        System.err.println("           -d -h <host> -p <port> -t <topic> --password <password>");
        if (arg != null) {
            System.err.println("Missing required argument " + arg);
        }
        // parser.formatHelpWith(new MyFormatter());
        parser.printHelpOn(System.err);
        System.exit(1);
    }
}
