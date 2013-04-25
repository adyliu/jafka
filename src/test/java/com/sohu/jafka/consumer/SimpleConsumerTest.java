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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.api.MultiFetchResponse;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringEncoder;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SimpleConsumerTest extends BaseJafkaServer {

    Jafka jafka;

    SimpleConsumer consumer;

    public SimpleConsumerTest() {
    }

    final int partitions = 3;

    @Before
    public void init() {
        if (jafka == null) {
            Properties props = new Properties();
            //force flush message to disk
            //we will fetch nothing while messages have note been flushed to disk
            props.put("log.flush.interval", "1");
            props.put("log.default.flush.scheduler.interval.ms", "100");//flush to disk every 100ms
            props.put("log.file.size", "5120");//5k for rolling
            props.put("num.partitions", "" + partitions);//default divided three partitions
            jafka = createJafka(props);
            sendSomeMessages(1000,"demo","test");
            flush(jafka);
            
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        if (consumer == null) {
            consumer = new SimpleConsumer("localhost", jafka.getPort(), 10 * 1000, 1024 * 1024);
        }
    }

    private void sendSomeMessages(int count,final String ...topics) {
        Properties producerConfig = new Properties();
        producerConfig.setProperty("broker.list", "0:localhost:" + jafka.getPort());
        producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
        int index = 0;
        boolean over = false;
        while (!over) {
            StringProducerData[] data = new StringProducerData[topics.length];
            for(int x=0;x<data.length;x++) {
                data[x] = new StringProducerData(topics[x]);
            }
            int batch = 50;
            while (batch-- > 0 && !over) {
                for(StringProducerData sd:data) {
                    sd.add(sd.getTopic()+"#message#"+(index++));
                }
                over = index >= count;
            }
            for(StringProducerData sd:data) {
                producer.send(sd);
            }
        }
        producer.close();
        System.out.println("send " + index + " messages");
    }
    
    @After
    public void destroy() throws Throwable {
        close(jafka);
        jafka = null;
        if (consumer != null) {
            consumer.close();
            consumer = null;
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
     * {@link com.sohu.jafka.consumer.SimpleConsumer#close()}.
     */
    @Test
    public void testClose() throws IOException {
        consumer.close();
        //
        //reconnect by itself
        testFetch();
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#fetch(com.sohu.jafka.api.FetchRequest)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public void testFetch() throws IOException {
        long offset = -1;
        int cnt = 0;
        for (int i = 0; i < partitions; i++) {
            FetchRequest request = new FetchRequest("demo", i, 0, 1000 * 1000);
            ByteBufferMessageSet messages = consumer.fetch(request);
            for (MessageAndOffset msg : messages) {
                //System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
                offset = Math.max(offset, msg.offset);
                cnt++;
            }
        }
        System.out.println("receive message count: " + cnt);
        assertTrue(offset > 0);
        assertTrue(cnt > 0);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#getOffsetsBefore(java.lang.String, int, long, int)}
     * .
     */
    @Test
    public void testGetOffsetsBefore() throws IOException {
        int size = 0;
        long maxoffset = -1;
        for (int i = 0; i < 3; i++) {
            long[] offsets = consumer.getOffsetsBefore("demo", i, OffsetRequest.LATES_TTIME, 100);
            size += offsets.length;
            if (offsets.length > 0 && offsets[0] > maxoffset) {
                maxoffset = offsets[0];
            }
        }
        System.out.println("demo largest offset: " + maxoffset);
        assertTrue(size > 0);
        assertTrue(maxoffset > 0);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#multifetch(java.util.List)}
     * .
     */
    @Test
    public void testMultifetch() throws IOException {

        long bigestOffset = -1;
        int cnt = 0;
        for (int i = 0; i < partitions; i++) {

            long demoOffset = 0;
            long testOffset = 0;
            boolean over = false;
            while (!over) {
                FetchRequest request1 = new FetchRequest("demo", i, demoOffset, 1000 * 1000);
                FetchRequest request2 = new FetchRequest("test", i, testOffset, 1000 * 1000);
                MultiFetchResponse responses = consumer.multifetch(Arrays.asList(request1, request2));
                assertEquals(2, responses.size());
                Iterator<ByteBufferMessageSet> iter = responses.iterator();
                assertTrue(iter.hasNext());
                ByteBufferMessageSet demoMessages = iter.next();
                assertTrue(iter.hasNext());
                ByteBufferMessageSet testMessages = iter.next();
                assertFalse(iter.hasNext());
                over = true;
                for (MessageAndOffset msg : demoMessages) {
                    //System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
                    bigestOffset = Math.max(bigestOffset, msg.offset);
                    cnt++;
                    demoOffset = msg.offset;
                    over = false;
                }
                for (MessageAndOffset msg : testMessages) {
                    //System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
                    bigestOffset = Math.max(bigestOffset, msg.offset);
                    cnt++;
                    testOffset = msg.offset;
                    over = false;
                }
            }
        }
        System.out.println("multifetch receive message count: " + cnt);
        assertTrue(bigestOffset > 0);
        assertTrue(cnt > 0);
    }

}
