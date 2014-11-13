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

import com.github.zkclient.ZkServer;
import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.DataLogCleaner;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.PortUtils;
import com.sohu.jafka.TestUtil;
import com.sohu.jafka.ZkServerTestUtil;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringDecoder;
import com.sohu.jafka.producer.serializer.StringEncoder;
import com.sohu.jafka.utils.ImmutableMap;
import com.sohu.jafka.utils.KV;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author adyliu (imxylz@gmail.com)
 */
public class ZookeeperConsumerConnector3Test extends BaseJafkaServer {

    private ZkServer zkServer;

    final int port = PortUtils.checkAvailablePort(2188);

    @Before
    public void createZkServer() throws IOException {
        //ZkServerTestUtil.setLogFileSize(1024);
        zkServer = ZkServerTestUtil.startZkServer(port);
    }

    @After
    public void destroy() {
        ZkServerTestUtil.closeZkServer(zkServer);
    }

    /**
     * Test method for
     * {@link ZookeeperConsumerConnector#createMessageStreams(Map, com.sohu.jafka.producer.serializer.Decoder)}
     * .
     */
    @Test
    public void testCreateMessageStreams() throws Exception {

        final int MAX_MESSAGE_SIZE = 1024;
        final BitSet bitSet = new BitSet(MAX_MESSAGE_SIZE);
        //create some jafka
        final int jafkaCount = 2;
        final int partition = 5;
        Jafka[] jafkas = new Jafka[jafkaCount];

        for (int i = 0; i < jafkaCount; i++) {
            Jafka jafka = createNewJafka(partition, i);
            jafkas[i] = jafka;
        }
        //
        //Thread.sleep(3000L);//waiting for server register
        //
        final Properties props = new Properties();
        props.setProperty("zk.connect", "localhost:" + port);
        props.setProperty("serializer.class", StringEncoder.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        //send some message
        final AtomicInteger messageIndex = new AtomicInteger(0);
        while (messageIndex.get() < 100) {
            StringProducerData data = new StringProducerData("demo");
            data.setKey("0");
            data.add("" + messageIndex.incrementAndGet());
            producer.send(data);
        }
        Thread.sleep(1000L);
        //
        //flush all servers
        for (Jafka jafka : jafkas) {
            flush(jafka);
        }
        //waiting for sending over
        Thread.sleep(1000L);
        {
            //new jafka server
            Jafka[] newjafkas = new Jafka[jafkas.length*2];
            System.arraycopy(jafkas,0,newjafkas,0,jafkas.length);
            for(int i=jafkas.length;i<newjafkas.length;i++){
                newjafkas[i] = createNewJafka(1,i);
            }
            jafkas = newjafkas;
        }

        //
        props.setProperty("groupid", "group1");
        //
        final AtomicInteger receiveCount = new AtomicInteger();
        final String[] consumerIds = {"consumer1", "consumer2", "consumer3", "consumer4"};
        KV<ExecutorService, ConsumerConnector> kv1 = createConsumer(props, 2, consumerIds[0], receiveCount, bitSet);
        KV<ExecutorService, ConsumerConnector> kv2 = createConsumer(props, 2, consumerIds[1], receiveCount, bitSet);

        Thread.sleep(1000L);
        KV<ExecutorService, ConsumerConnector> kv3 = createConsumer(props, 2, consumerIds[2], receiveCount, bitSet);

        // loop forever
        final AtomicBoolean stop = new AtomicBoolean(false);
        new Thread() {
            @Override
            public void run() {
                final String consumerId4 = consumerIds[3];
                while (!stop.get()) {
                    try {
                        KV<ExecutorService, ConsumerConnector> kv4 = createConsumer(props, 2, consumerId4, receiveCount, bitSet);
                        Thread.sleep(1000L);
                        kv4.v.close();
                        kv4.k.shutdown();
                        Thread.sleep(1000L);

                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }.start();
        Producer<String, String> producer2 = new Producer<String, String>(producerConfig);
        while (messageIndex.get() < MAX_MESSAGE_SIZE) {
            if (messageIndex.get() < MAX_MESSAGE_SIZE) {
                StringProducerData data = new StringProducerData("demo");
                for (int i = 0; i < 3 && messageIndex.get() < MAX_MESSAGE_SIZE; i++) {
                    data.add("" + messageIndex.incrementAndGet());
                }
                producer.send(data);
            }
            if (messageIndex.get() < MAX_MESSAGE_SIZE) {
                StringProducerData data = new StringProducerData("demo");
                for (int i = 0; i < 3 && messageIndex.get() < MAX_MESSAGE_SIZE; i++) {
                    data.add("" + messageIndex.incrementAndGet());
                }
                producer2.send(data);
            }
            Thread.sleep(5L);
        }
        stop.set(true);
        //
        TestUtil.waitUntil(messageIndex.get(), new Callable<Integer>() {
            public Integer call() throws Exception {
                return receiveCount.get();
            }
        }, TimeUnit.MINUTES, 5);
        producer.close();
        producer2.close();
        System.out.println(String.format("message sent/received %s =? %s", messageIndex.get(), receiveCount.get()));
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
        assertEquals(messageIndex.get(), receiveCount.get());
    }

    private Jafka createNewJafka(int partition, int i) {
        Properties serverProperties = new Properties();
        serverProperties.setProperty("enable.zookeeper", "true");
        serverProperties.setProperty("zk.connect", "localhost:" + port);
        serverProperties.setProperty("port", String.valueOf(PortUtils.checkAvailablePort(9092)));
        serverProperties.setProperty("brokerid", "" + i);
        serverProperties.setProperty("num.partitions", "" + partition);
        //serverProperties.setProperty("topic.partition.count.map", "demo:10");
        serverProperties.setProperty("log.dir", DataLogCleaner.defaultDataLogPath + "/jafka" + i);
        return createJafka(serverProperties);
    }

    private synchronized boolean setBit(BitSet bitSet, int i) {
        i--;
        if (!bitSet.get(i)) {
            bitSet.set(i);
            return i < bitSet.size();
        }
        return false;
    }

    private KV<ExecutorService, ConsumerConnector> createConsumer(Properties oldProps, int topicCount,
                                                                  final String consumerId,
                                                                  final AtomicInteger receiveCount,
                                                                  final BitSet bitSet
    ) throws Exception {
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
                        receiveCount.incrementAndGet();
                        int i = Integer.parseInt(message);
                        if (!setBit(bitSet, i)) {
                            System.err.println(i + " NOT IN (1," + bitSet.size()+") OR DUPLICATION");
                        }
                    }
                }
            });
        }
        //
        return new KV<ExecutorService, ConsumerConnector>(service, connector);
    }
}
