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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SimpleConsumerTest extends BaseJafkaServer {

    Jafka jafka;

    SimpleConsumer consumer;
    final static AtomicInteger count = new AtomicInteger();
    public SimpleConsumerTest() {
        System.out.println("build instance "+count.incrementAndGet());
    }

    @Before
    public void init() {
        if (jafka == null) {
            Properties props = new Properties();
            //force flush message to disk
            //we will fetch nothing while messages have note been flushed to disk
            props.put("log.flush.interval", "100");
            props.put("log.file.size", "5120");//10k for rolling
            props.put("num.partitions", "3");//default divided three partitions
            jafka = createJafka(props);
            Properties producerConfig = new Properties();
            producerConfig.setProperty("broker.list", "0:localhost:9092");
            producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
            Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
            StringProducerData data = new StringProducerData("demo");
            StringProducerData data2 = new StringProducerData("demo2");
            for (int i = 0; i < 100; i++) {
                data.add("message#" + i);
                data2.add("message#demo2#" + i);
            }
            for(int i=0;i<10;i++) {
                producer.send(data);
                producer.send(data2);
            }
            producer.close();
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1L));//waiting for log created
        }
        if (consumer == null) {
            consumer = new SimpleConsumer("localhost", 9092, 10 * 1000, 1024 * 1024);
        }
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

    /**
     * Test method for {@link com.sohu.jafka.consumer.SimpleConsumer#close()}.
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
     * {@link com.sohu.jafka.consumer.SimpleConsumer#fetch(com.sohu.jafka.api.FetchRequest)}.
     * 
     * @throws IOException
     */
    @Test
    public void testFetch() throws IOException {
        FetchRequest request = new FetchRequest("demo", 0, 0, 100 * 1000);
        ByteBufferMessageSet messages = consumer.fetch(request);
        long offset = 0;
        for (MessageAndOffset msg : messages) {
            System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
            offset = msg.offset;
        }
        assertTrue(offset > 0);
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
        for(int i=0;i<3;i++) {
            long[] offsets = consumer.getOffsetsBefore("demo", i, OffsetRequest.LATES_TTIME, 100);
            size += offsets.length;
            if(offsets.length >0 && offsets[0]>maxoffset) {
                maxoffset = offsets[0];
            }
        }
        System.out.println("demo largest offset: "+maxoffset);
        assertTrue(size > 0);
        assertTrue(maxoffset > 0);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#multifetch(java.util.List)}.
     */
    @Test
    public void testMultifetch() throws IOException {
        FetchRequest request1 = new FetchRequest("demo", 0, 0, 100 * 1000);
        FetchRequest request2 = new FetchRequest("demo2", 0, 0, 100 * 1000);
        MultiFetchResponse responses = consumer.multifetch(Arrays.asList(request1, request2));
        for (ByteBufferMessageSet messages : responses) {
            long offset = 0;
            for (MessageAndOffset msg : messages) {
                System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
                offset = msg.offset;
            }
            assertTrue(offset > 0);
        }

    }

}
