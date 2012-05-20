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

package com.sohu.jafka.consumer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.zkclient.ZkServer;
import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.DataLogCleaner;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.ZkServerTestUtil;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringDecoder;
import com.sohu.jafka.producer.serializer.StringEncoder;
import com.sohu.jafka.utils.ImmutableMap;

/**
 * @author adyliu (imxylz@gmail.com)
 */
public class ZookeeperConsumerConnectorTest extends BaseJafkaServer {

    private ZkServer zkServer;

    final int port = 2188;

    @Before
    public void createZkServer() throws IOException {
        zkServer = ZkServerTestUtil.startZkServer(port);
    }

    @After
    public void destroy() {
        ZkServerTestUtil.closeZkServer(zkServer);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.ZookeeperConsumerConnector#createMessageStreams(java.util.Map, com.sohu.jafka.producer.serializer.Decoder)}
     * .
     */
    @Test
    public void testCreateMessageStreams() throws Exception {

        //create some jafka
        final int jafkaCount = 4;
        final int partition = 2;
        Jafka[] jafkas = new Jafka[jafkaCount];
        for (int i = 0; i < 4; i++) {
            Properties serverProperties = new Properties();
            serverProperties.setProperty("enable.zookeeper", "true");
            serverProperties.setProperty("zk.connect", "localhost:" + port);
            serverProperties.setProperty("port", String.valueOf(9092+i));
            serverProperties.setProperty("brokerid", "" + i);
            serverProperties.setProperty("num.partitions", ""+partition);
            serverProperties.setProperty("log.dir", DataLogCleaner.defaultDataLogPath + "/jafka" + i);
            Jafka jafka = createJafka(serverProperties);
            jafkas[i] = jafka;
        }
        //
        Thread.sleep(3000L);//waiting for server register
        //
        Properties props = new Properties();
        props.setProperty("zk.connect", "localhost:" + port);
        props.setProperty("serializer.class", StringEncoder.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        //send some message
        final int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            producer.send(new StringProducerData("demo").add("message#" + i));
        }
        Thread.sleep(1000L);
        //
        //flush all servers
        for (Jafka jafka : jafkas) {
            flush(jafka);
        }
        //waiting for sending over
        Thread.sleep(1000L);
        producer.close();
        //
        props.setProperty("groupid", "group1");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector connector = Consumer.create(consumerConfig);
        final int topicCount = jafkaCount*partition;
        Map<String, List<MessageStream<String>>> map = connector.createMessageStreams(
                ImmutableMap.of("demo", topicCount), new StringDecoder());
        assertEquals(1, map.size());
        List<MessageStream<String>> streams = map.get("demo");
        assertEquals(topicCount, streams.size());
        final ExecutorService service = Executors.newFixedThreadPool(topicCount);
        final AtomicInteger receiveCount = new AtomicInteger();
        int index = 0;
        for (final MessageStream<String> stream : streams) {
            final int streamIndex = index++;
            service.submit(new Runnable() {

                public void run() {
                    for (String message : stream) {
                        System.out.println(streamIndex+" => "+message);
                        receiveCount.incrementAndGet();
                    }
                }
            });
        }
        //
        Thread.sleep(5000L);
        connector.close();
        //close all servers
        for (Jafka jafka : jafkas) {
            close(jafka);
        }
        //
        service.shutdown();
        service.awaitTermination(5, TimeUnit.SECONDS);
        //
        assertEquals(messageCount, receiveCount.get());
        //
    }

}
