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

package com.sohu.jafka.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.admin.AdminOperation;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringEncoder;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
public class AdminOperationTest extends BaseJafkaServer {

    Jafka jafka;

    final int partitions = 3;

    final String password = "jafka";

    private AdminOperation admin;

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
            props.put("password", "plain:" + password);
            jafka = createJafka(props);
            sendSomeMessages(1000, "demo", "test");
            flush(jafka);

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        if (admin == null) {
            admin = new AdminOperation("localhost", jafka.getPort());
        }
    }

    private void sendSomeMessages(int count, final String... topics) {
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
            int batch = 50;
            while (batch-- > 0 && !over) {
                for (StringProducerData sd : data) {
                    sd.add(sd.getTopic() + "#message#" + (index++));
                }
                over = index >= count;
            }
            for (StringProducerData sd : data) {
                producer.send(sd);
            }
        }
        producer.close();
    }

    @After
    public void destroy() throws Throwable {
        close(jafka);
        jafka = null;
        if (admin != null) {
            admin.close();
            admin = null;
        }
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.admin.AdminOperation#createPartitions(java.lang.String, int, boolean)}
     * .
     */
    @Test
    public void testCreatePartitions() throws IOException {
        int size = admin.createPartitions("demo", partitions - 1, false);
        assertEquals(partitions, size);
        size = admin.createPartitions("demo", partitions + 1, false);
        assertEquals(partitions, size);
        size = admin.createPartitions("demo", partitions + 3, true);
        assertEquals(partitions+3, size);
        //
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.admin.AdminOperation#deleteTopic(java.lang.String, java.lang.String)}
     * .
     */
    @Test
    public void testDeleteTopic() throws IOException {
        int currentPartitions = admin.createPartitions("demo",partitions-1,false);
        int count = admin.deleteTopic("demo", password);
        assertTrue(count > 0 && count == currentPartitions);
    }

}
