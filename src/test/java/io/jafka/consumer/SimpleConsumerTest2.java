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

import io.jafka.BaseJafkaServer;
import io.jafka.Jafka;
import io.jafka.PortUtils;
import io.jafka.api.FetchRequest;
import io.jafka.api.MultiFetchResponse;
import io.jafka.message.ByteBufferMessageSet;
import io.jafka.message.MessageAndOffset;
import io.jafka.producer.Producer;
import io.jafka.producer.ProducerConfig;
import io.jafka.producer.StringProducerData;
import io.jafka.producer.serializer.StringEncoder;
import io.jafka.utils.Closer;
import io.jafka.utils.Gateway;
import io.jafka.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SimpleConsumerTest2 extends BaseJafkaServer {

    Jafka jafka;

    SimpleConsumer consumer;

    public SimpleConsumerTest2() {
    }

    final int partitions = 3;
    final int INIT_MESSAGE_COUNT = 27;
    final int MESSAGE_BATCH_SIZE = 5;
    final int httpPort = PortUtils.checkAvailablePort(9093);
    final int gatewayPort = PortUtils.checkAvailablePort(9095);
    private Gateway gateway;

    @Before
    public void init() throws IOException{
        if (jafka == null) {

            Properties props = new Properties();
            //force flush message to disk
            //we will fetch nothing while messages have note been flushed to disk
            props.put("log.flush.interval", "1");
            props.put("log.default.flush.scheduler.interval.ms", "100");//flush to disk every 100ms
            props.put("log.file.size", "5120");//5k for rolling
            props.put("num.partitions", "" + partitions);//default divided three partitions
            props.put("http.port",""+httpPort);
            jafka = createJafka(props);
            sendSomeMessages(INIT_MESSAGE_COUNT, "demo", "test");

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));//waiting to receive all message
            flush(jafka);//force flush all logs to the disk
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        if (consumer == null) {
            gateway = new Gateway(gatewayPort,jafka.getPort());
            gateway.start();
            consumer = new SimpleConsumer("localhost", gatewayPort, 100, 1000*1000);
        }
    }

    private void sendSomeMessages(final int count, final String... topics) {
        Properties producerConfig = new Properties();
        producerConfig.setProperty("broker.list", "0:localhost:" + jafka.getPort());
        producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
        int index = 0;
        boolean over = false;
        while (!over) {
            StringProducerData[] data = new StringProducerData[topics.length];
            for (int x = 0; x < data.length; x++) {
                data[x] = new StringProducerData(topics[x]);
            }
            int batch = MESSAGE_BATCH_SIZE;
            while (batch-- > 0 && !over) {
                for (StringProducerData sd : data) {
                    if(!(over = index >= count)) {
                        sd.add(sd.getTopic() + "#message#" + (index++));
                    }
                }
            }
            for (StringProducerData sd : data) {
                producer.send(sd);
            }
        }
        Closer.closeQuietly(producer);
        logger.info("SEND_MESSAGE_COUNT {}/{}", index,count);
        assertEquals(count,index);
    }

    @After
    public void destroy() throws Throwable {
        close(jafka);
        jafka = null;
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        if(gateway != null){
            gateway.stop();
            gateway = null;
        }
    }


//    @Test
//    public void testCreatePartitions() throws IOException{
//        int size = consumer.createPartitions("demo", partitions-1, false);
//        assertEquals(partitions, size);
//        size = consumer.createPartitions("demo", partitions+1, false);
//        assertEquals(partitions, size);
//        size = consumer.createPartitions("demo", partitions+3, true);
//        assertEquals(partitions, size);
//        //
//        final String largePartitionTopic = "largepartition";
//        size = consumer.createPartitions(largePartitionTopic, partitions+5, true);
//        assertEquals(partitions+5, size);
//        sendSomeMessages(1000, largePartitionTopic);
//    }




    /**
     * Test method for
     * {@link SimpleConsumer#multifetch(java.util.List)}
     * .
     */
    @Test
    public void testMultifetch() throws IOException {

        long bigestOffset = -1;
        int total = 0, democnt = 0, testcnt = 0;
        //

        //
        int loop = 10;
        while (loop-- > 0 && total < INIT_MESSAGE_COUNT) {
            for (int i = 0; i < partitions && total < INIT_MESSAGE_COUNT; i++) {

                long demoOffset = 0;
                long testOffset = 0;

                FetchRequest request1 = new FetchRequest("demo", i, demoOffset, 64000);
                FetchRequest request2 = new FetchRequest("test", i, testOffset, 64000);
                MultiFetchResponse responses = consumer.multifetch(Arrays.asList(request1, request2));
                assertEquals(2, responses.size());
                Iterator<ByteBufferMessageSet> iter = responses.iterator();
                assertTrue(iter.hasNext());
                ByteBufferMessageSet demoMessages = iter.next();
                assertTrue(iter.hasNext());
                ByteBufferMessageSet testMessages = iter.next();
                assertFalse(iter.hasNext());
                //
                for (MessageAndOffset msg : demoMessages) {
                    System.out.printf("Receive offset=%s data=%s\n",msg.offset,Utils.toString(msg.message.payload(), "UTF-8"));
                    bigestOffset = Math.max(bigestOffset, msg.offset);
                    total++;
                    democnt++;
                    demoOffset = msg.offset;
                }
                for (MessageAndOffset msg : testMessages) {
                    System.out.printf("Receive offset=%s data=%s\n",msg.offset,Utils.toString(msg.message.payload(), "UTF-8"));
                    bigestOffset = Math.max(bigestOffset, msg.offset);
                    total++;
                    testcnt++;
                    testOffset = msg.offset;
                }
                gateway.supsend(TimeUnit.SECONDS.toMillis(1));
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                gateway.supsend(0);

            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
        consumer.close();
        logger.info("multifetch receive message count: {}, demo={}, test={}", total, democnt, testcnt);
        assertTrue(bigestOffset > 0);
        assertEquals(total,INIT_MESSAGE_COUNT);
    }

}
