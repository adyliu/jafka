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
import java.util.Properties;
import java.util.Random;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkInterruptedException;
import com.sohu.jafka.consumer.Consumer;
import com.sohu.jafka.consumer.ConsumerConfig;
import com.sohu.jafka.consumer.ConsumerConnector;
import com.sohu.jafka.consumer.MessageStream;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.producer.serializer.MessageEncoders;
import com.sohu.jafka.utils.Closer;
import com.sohu.jafka.utils.ImmutableMap;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ConsoleConsumer {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> topicIdOpt = parser.accepts("topic", "REQUIRED: The topic id to consumer on.")//
                .withRequiredArg().describedAs("topic").ofType(String.class);
        final ArgumentAcceptingOptionSpec<String> zkConnectOpt = parser
                .accepts("zookeeper",
                        "REQUIRED: The connection string for the zookeeper connection in the form host:port.  Multiple URLS can be given to allow fail-over.")//
                .withRequiredArg().describedAs("urls").ofType(String.class);
        final ArgumentAcceptingOptionSpec<String> groupIdOpt = parser.accepts("group", "The group id to consume on.")//
                .withRequiredArg().describedAs("gid").defaultsTo("console-consumer-" + new Random().nextInt(100000)).ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")//
                .withRequiredArg().describedAs("size").ofType(Integer.class).defaultsTo(1024 * 1024);
        ArgumentAcceptingOptionSpec<Integer> socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")//
                .withRequiredArg().describedAs("size").ofType(Integer.class).defaultsTo(2 * 1024 * 1024);
        ArgumentAcceptingOptionSpec<Integer> consumerTimeoutMsOpt = parser
                .accepts("consumer-timeout-ms", "consumer throws timeout exception after waiting this much " + "of time without incoming messages")//
                .withRequiredArg().describedAs("prop").ofType(Integer.class).defaultsTo(-1);
        ArgumentAcceptingOptionSpec<String> messageFormatterOpt = parser
                .accepts("formatter", "The name of a class to use for formatting jafka messages for display.").withRequiredArg().describedAs("class")
                .ofType(String.class).defaultsTo(NewlineMessageFormatter.class.getName());
        //ArgumentAcceptingOptionSpec<String> messageFormatterArgOpt = parser.accepts("property")//
       //         .withRequiredArg().describedAs("prop").ofType(String.class);
        OptionSpecBuilder resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, "
                + "start with the earliest message present in the log rather than the latest message.");
        ArgumentAcceptingOptionSpec<Integer> autoCommitIntervalOpt = parser
                .accepts("autocommit.interval.ms", "The time interval at which to save the current offset in ms")//
                .withRequiredArg().describedAs("ms").ofType(Integer.class).defaultsTo(10 * 1000);
       // ArgumentAcceptingOptionSpec<Integer> maxMessagesOpt = parser
         //       .accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")//
         //       .withRequiredArg().describedAs("num_messages").ofType(Integer.class);
        OptionSpecBuilder skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, "
                + "skip it instead of halt.");
        //

        final OptionSet options = tryParse(parser, args);
        checkRequiredArgs(parser, options, topicIdOpt, zkConnectOpt);
        //
        Properties props = new Properties();
        props.put("groupid", options.valueOf(groupIdOpt));
        props.put("socket.buffersize", options.valueOf(socketBufferSizeOpt).toString());
        props.put("socket.buffersize", options.valueOf(socketBufferSizeOpt).toString());
        props.put("fetch.size", options.valueOf(fetchSizeOpt).toString());
        props.put("auto.commit", "true");
        props.put("autocommit.interval.ms", options.valueOf(autoCommitIntervalOpt).toString());
        props.put("autooffset.reset", options.has(resetBeginningOpt) ? "smallest" : "largest");
        props.put("zk.connect", options.valueOf(zkConnectOpt));
        props.put("consumer.timeout.ms", options.valueOf(consumerTimeoutMsOpt).toString());
        //
        //
        final ConsumerConfig config = new ConsumerConfig(props);
        final boolean skipMessageOnError = options.has(skipMessageOnErrorOpt);
        final String topic = options.valueOf(topicIdOpt);
        @SuppressWarnings("unchecked")
        final Class<MessageFormatter> messageFormatterClass = (Class<MessageFormatter>) Class.forName(options.valueOf(messageFormatterOpt));

        //final int maxMessages = options.has(maxMessagesOpt) ? options.valueOf(maxMessagesOpt).intValue() : -1;
        final ConsumerConnector connector = Consumer.create(config);
        if (options.has(resetBeginningOpt)) {
            tryCleanupZookeeper(options.valueOf(zkConnectOpt), options.valueOf(groupIdOpt));
        }
        //
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                Closer.closeQuietly(connector);
                if (!options.has(groupIdOpt)) {
                    tryCleanupZookeeper(options.valueOf(zkConnectOpt), options.valueOf(groupIdOpt));
                }
            }
        });
        //
        MessageStream<Message> stream = connector.createMessageStreams(ImmutableMap.of(topic, 1), new MessageEncoders()).get(topic).get(0);
        final MessageFormatter formatter = messageFormatterClass.newInstance();
        //formatter.init(props);
        //
        try {
            for (Message message : stream) {
                try {
                    formatter.writeTo(message, System.out);
                } catch (RuntimeException e) {
                    if (skipMessageOnError) {
                        System.err.println(e.getMessage());
                    } else {
                        throw e;
                    }
                    //
                    if (System.out.checkError()) {
                        System.err.println("Unable to write to standard out, closing consumer.");
                        formatter.close();
                        connector.close();
                        System.exit(1);
                    }
                }
            }
        } finally {
            System.out.flush();
            formatter.close();
            connector.close();
        }
    }

    static OptionSet tryParse(OptionParser parser, String[] args) {
        try {
            return parser.parse(args);
        } catch (OptionException e) {
            e.printStackTrace();
            return null;
        }
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

    static void tryCleanupZookeeper(String zkConnect, String groupId) {
        try {
            String dir = "/consumers/" + groupId;
            ZkClient zk = new ZkClient(zkConnect, 30 * 1000, 30 * 1000);
            zk.deleteRecursive(dir);
            zk.close();
        } catch (ZkInterruptedException e) {
            e.printStackTrace();
        }
    }
}
