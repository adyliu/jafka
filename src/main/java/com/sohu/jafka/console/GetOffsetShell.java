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

import com.sohu.jafka.consumer.SimpleConsumer;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class GetOffsetShell {


    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> urlOpt = parser.accepts("server", "REQUIRED: the jafka request uri")//
                .withRequiredArg().describedAs("jafka://ip:port").ofType(String.class);
        ArgumentAcceptingOptionSpec<String> topicOpt = parser
                .accepts("topic", "REQUIRED: The topic to get offset from.")//
                .withRequiredArg().describedAs("topic").ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> partitionOpt = parser.accepts("partition", "partition id")//
                .withRequiredArg().describedAs("partition_id").ofType(Integer.class).defaultsTo(0);
        ArgumentAcceptingOptionSpec<Long> timeOpt = parser
                .accepts("time",
                        "unix time(ms) of the offsets.         -1: lastest; -2: earliest; unix million seconds: offset before this time")//
                .withRequiredArg().describedAs("unix_time").ofType(Long.class).defaultsTo(-1L);
        ArgumentAcceptingOptionSpec<Integer> noffsetsOpt = parser.accepts("offsets", "number of offsets returned")//
                .withRequiredArg().describedAs("count").ofType(Integer.class).defaultsTo(1);
        OptionSet options = parser.parse(args);
        checkRequiredArgs(parser, options, urlOpt, topicOpt, timeOpt);

        URI uri = new URI(options.valueOf(urlOpt));
        String topic = options.valueOf(topicOpt);
        int partition = options.valueOf(partitionOpt).intValue();
        long time = options.valueOf(timeOpt).longValue();
        int noffsets = options.valueOf(noffsetsOpt).intValue();
        SimpleConsumer consumer = new SimpleConsumer(uri.getHost(), uri.getPort(), 10000, 100 * 1000);
        try {
            long[] offsets = consumer.getOffsetsBefore(topic, partition, time, noffsets);
            System.out.println("get " + offsets.length + " result");
            for (long offset : offsets) {
                System.out.println(offset);
            }
        } finally {
            consumer.close();
        }
    }

    static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec<?>... optionSepcs)
            throws IOException {
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
