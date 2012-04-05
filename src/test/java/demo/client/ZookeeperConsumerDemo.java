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

package demo.client;


import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sohu.jafka.consumer.Consumer;
import com.sohu.jafka.consumer.ConsumerConfig;
import com.sohu.jafka.consumer.ConsumerConnector;
import com.sohu.jafka.consumer.MessageStream;
import com.sohu.jafka.producer.serializer.StringDecoder;
import com.sohu.jafka.utils.ImmutableMap;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-19
 */
public class ZookeeperConsumerDemo {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("zk.connect", "localhost:2181");
        props.put("groupid", "test_group");
        //
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector connector = Consumer.create(consumerConfig);
        //
        Map<String, List<MessageStream<String>>> topicMessageStreams = connector.createMessageStreams(ImmutableMap.of("demo", 2), new StringDecoder());
        List<MessageStream<String>> streams = topicMessageStreams.get("demo");
        //
        ExecutorService executor = Executors.newFixedThreadPool(2);
        final AtomicInteger count = new AtomicInteger();
        for (final MessageStream<String> stream : streams) {
            executor.submit(new Runnable() {

                public void run() {
                    for (String message : stream) {
                        System.out.println(count.incrementAndGet() + " => " + message);
                    }
                }
            });
        }
        //
        executor.awaitTermination(1, TimeUnit.HOURS);
    }

}
