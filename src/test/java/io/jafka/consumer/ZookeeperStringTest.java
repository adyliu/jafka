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

package io.jafka.consumer;

import com.github.zkclient.ZkServer;
import io.jafka.BaseJafkaServer;
import io.jafka.DataLogCleaner;
import io.jafka.Jafka;
import io.jafka.PortUtils;
import io.jafka.TestUtil;
import io.jafka.ZkServerTestUtil;
import io.jafka.producer.StringProducers;
import io.jafka.producer.StringProducerData;
import io.jafka.producer.serializer.Decoder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author adyliu (imxylz@gmail.com)
 */
public class ZookeeperStringTest extends BaseJafkaServer {

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
     * {@link ZookeeperConsumerConnector#createMessageStreams(Map, Decoder)}
     * .
     */
    @Test
    public void testCreateMessageStreams() throws Exception {

        //create some jafka
        final int jafkaCount = 2;
        final int partition = 1;
        Jafka[] jafkas = new Jafka[jafkaCount];

        for (int i = 0; i < jafkaCount; i++) {
            Properties serverProperties = new Properties();
            serverProperties.setProperty("enable.zookeeper", "true");
            serverProperties.setProperty("zk.connect", "localhost:" + port);
            serverProperties.setProperty("port", String.valueOf(PortUtils.checkAvailablePort(9092)));
            serverProperties.setProperty("brokerid", "" + i);
            serverProperties.setProperty("num.partitions", "" + partition);
            serverProperties.setProperty("log.dir", DataLogCleaner.defaultDataLogPath + "/jafka" + i);
            Jafka jafka = createJafka(serverProperties);
            jafkas[i] = jafka;
        }
        //
        final String zookeeperConfig = "localhost:" + port;
        final String topicName = "demo";

        StringProducers ps = StringProducers.buildGlobal(zookeeperConfig);



        //send some message
        final int messageCount = 100;
        final HashSet<String> messages = new HashSet<String>(messageCount);
        for (int i = 0; i < messageCount; i++) {
            String msg = "message#" + i;

            ps.send(new StringProducerData(topicName,msg));
            if(i ==0) {
                //Thread.sleep(2000L);//waiting broker register this topic
            }
            messages.add(msg);
        }
        Thread.sleep(1000L);
        //
        //flush all servers
        for (Jafka jafka : jafkas) {
            flush(jafka);
        }
        //waiting for sending over
        //Thread.sleep(1000L);
        Assert.assertEquals(messageCount, messages.size());
        //
        final AtomicInteger count = new AtomicInteger(0);
        StringConsumers sc = StringConsumers.buildConsumer(zookeeperConfig, topicName, "mygroup", new IMessageListener<String>() {
            @Override
            public void onMessage(String message) {
                //System.out.println(count.getAndIncrement()+" -> "+message);
                //Assert.assertEquals(message,"message#"+count.getAndIncrement());
                messages.remove(message);
                count.getAndIncrement();
            }
        });
        TestUtil.waitUntil(messageCount, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return count.get();
            }
        },TimeUnit.SECONDS,10);
        try {
            Assert.assertEquals(messageCount, count.get());
            if (messages.size() > 0) {
                for(String m: messages){
                    logger.error("message=>"+m.length()+":"+m);
                }
                logger.error("what's this? =="+messages+"== size="+messages.size());
            }
            Assert.assertEquals(0, messages.size());
        }finally {
            sc.close();
            for(Jafka jafka: jafkas){
                close(jafka);
            }
        }
    }

}
