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

import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.ProducerData;
import com.sohu.jafka.producer.serializer.StringEncoder;
import com.sohu.jafka.utils.Closer;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ConsoleProducer {


    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        final ArgumentAcceptingOptionSpec<String> topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")//
                .withRequiredArg().describedAs("topic").ofType(String.class);
        final ArgumentAcceptingOptionSpec<String> zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: zokkeper connection string, form: HOST:PORT[/CHROOT]")//
                .withRequiredArg().describedAs("connection_string").ofType(String.class);
        final ArgumentAcceptingOptionSpec<String> brokerListOpt = parser.accepts("broker-list", "REQUIRED: broker list, form: 0:localhost:9092")//
                .withRequiredArg().describedAs("broker_list").ofType(String.class);
        final ArgumentAcceptingOptionSpec<String> messageReaderOpt = parser.accepts("message-encoder", "The class name of the message encoder")//
                .withRequiredArg().describedAs("encoder_class").ofType(String.class).defaultsTo(LineMessageReader.class.getName());

        //
        OptionSet options = parser.parse(args);
        if (options.has(zkConnectOpt) && options.has(brokerListOpt)) {
            System.err.println("Only broker-list or zookeeper config");
            parser.printHelpOn(System.err);
            return;
        }
        if (!options.has(zkConnectOpt) && !options.has(brokerListOpt)) {
            System.err.println("Missing required argument broker-list or zookeeper");
            parser.printHelpOn(System.err);
            return;
        }
        ////////////////////////////////////////////////////////////////////
        final Properties props = new Properties();
        if (options.has(zkConnectOpt)) {
            checkRequiredArgs(parser, options, topicOpt, zkConnectOpt);
            props.put("zk.connect", options.valueOf(zkConnectOpt));
        } else {
            checkRequiredArgs(parser, options, topicOpt, brokerListOpt);
            props.put("broker.list", options.valueOf(brokerListOpt));
        }

        final String topic = options.valueOf(topicOpt);
        props.put("serializer.class", StringEncoder.class.getName());

        MessageReader reader = (MessageReader) Class.forName(options.valueOf(messageReaderOpt)).newInstance();
        reader.init(System.in, null);
        final Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                Closer.closeQuietly(producer);
            }
        });
        //

        String message = null;
        while ((message = reader.readMessage()) != null) {
            if (message.length() == 0) {
                break;
            }
            producer.send(new ProducerData<String, String>(topic, message));
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

}
