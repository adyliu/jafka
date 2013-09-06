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

import com.sohu.jafka.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.zkclient.ZkServer;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringDecoder;
import com.sohu.jafka.producer.serializer.StringEncoder;
import com.sohu.jafka.utils.ImmutableMap;
import com.sohu.jafka.utils.KV;

/**
 * @author adyliu (imxylz@gmail.com)
 */
public class ZookeeperConsumerConnectorTest extends BaseJafkaServer {

    private ZkServer zkServer;

    final int port = PortUtils.checkAvailablePort(2188);

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
        final int jafkaCount = 2;
        final int partition = 5;
        Jafka[] jafkas = new Jafka[jafkaCount];

        for (int i = 0; i < jafkaCount; i++) {
            Properties serverProperties = new Properties();
            serverProperties.setProperty("enable.zookeeper", "true");
            serverProperties.setProperty("zk.connect", "localhost:" + port);
            serverProperties.setProperty("port", String.valueOf(PortUtils.checkAvailablePort(9092)));
            serverProperties.setProperty("brokerid", "" + i);
            serverProperties.setProperty("num.partitions", "" + partition);
            serverProperties.setProperty("topic.partition.count.map", "demo:5");//FIXME:
            serverProperties.setProperty("log.dir", DataLogCleaner.defaultDataLogPath + "/jafka" + i);
            Jafka jafka = createJafka(serverProperties);
            jafkas[i] = jafka;
        }
        //
        //Thread.sleep(3000L);//waiting for server register
        //
        Properties props = new Properties();
        props.setProperty("zk.connect", "localhost:" + port);
        props.setProperty("serializer.class", StringEncoder.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        //send some message
        final int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            String msg = "message#" + i+"#"+System.currentTimeMillis();
            StringProducerData data = new StringProducerData("demo");
            data.setKey(msg);
            data.add(msg);
            producer.send(data);
            if(i ==0) {
                Thread.sleep(2000L);//waiting broker register this topic
            }
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
        //
        final AtomicInteger receiveCount = new AtomicInteger();
        KV<ExecutorService, ConsumerConnector> kv1 = createConsumer(props, 2, "photopark_sohu-1337926225553-6e3daba6",receiveCount);
        KV<ExecutorService, ConsumerConnector> kv2 = createConsumer(props, 2, "photopark_sohu-1337936606600-9c8a98c4",receiveCount);

        Thread.sleep(3000L);
        KV<ExecutorService, ConsumerConnector> kv3 = createConsumer(props, 2, "photopark_sohu-1337936716354-ab3c9481",receiveCount);
        while(receiveCount.get()<messageCount) {
        }
        System.out.println("receive message => "+receiveCount.get());
        kv1.v.close();
        kv1.k.shutdown();
        kv2.v.close();
        kv2.k.shutdown();
        kv3.v.close();
        kv3.k.shutdown();
        //close all servers
        for (Jafka jafka : jafkas) {
            close(jafka);
        }
        //
        kv1.k.awaitTermination(30, TimeUnit.SECONDS);
        //
        //assertEquals(messageCount, receiveCount.get());
        //
    }

    private KV<ExecutorService, ConsumerConnector> createConsumer(Properties oldProps, int topicCount,
            final String consumerId,final AtomicInteger receiveCount) throws Exception {
        Properties props = new Properties();
        props.putAll(oldProps);
        props.setProperty("consumerid", consumerId);
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector connector = Consumer.create(consumerConfig);
        Map<String, List<MessageStream<String>>> map = connector.createMessageStreams(
                ImmutableMap.of("demo", topicCount), new StringDecoder());
        assertEquals(1, map.size());
        List<MessageStream<String>> streams = map.get("demo");
        assertEquals(topicCount, streams.size());
        final ExecutorService service = Executors.newFixedThreadPool(topicCount);
        for (final MessageStream<String> stream : streams) {
            service.submit(new Runnable() {
                public void run() {
                    for (String message : stream) {
                        //System.out.println(consumerId +  " => " + message);
                        receiveCount.incrementAndGet();
                    }
                }
            });
        }
        //
        return new KV<ExecutorService, ConsumerConnector>(service, connector);
    }
}
