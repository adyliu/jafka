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

package com.sohu.jafka.producer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.consumer.SimpleConsumer;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.producer.serializer.StringEncoder;
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ProducerTest extends BaseJafkaServer {

    /**
     * Test method for
     * {@link com.sohu.jafka.producer.Producer#send(com.sohu.jafka.producer.ProducerData)}.
     */
    @Test
    public void testSend() throws IOException {
        Jafka jafka = createJafka();
        Properties producerConfig = new Properties();
        producerConfig.setProperty("broker.list", "0:localhost:9092");
        producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
        String[] messages = { "Hello, jafka", "https://github.com/adyliu/jaka", "Open Source", "Fast Messaging System" };

        StringProducerData producerData = new StringProducerData("demo");
        for (String message : messages) {
            producerData.add(message);
        }
        final long[] offsets = producer.send(producerData);
        producer.close();
        flush(jafka);//force flush data
        //
        assertEquals(messages.length,offsets.length);
        SimpleConsumer consumer = new SimpleConsumer("localhost", 9092);
        for (int i = 0; i < messages.length; i++) {
            int index = i;
            for (MessageAndOffset messageAndOffset : consumer.fetch(new FetchRequest("demo", 0, offsets[i]))) {
                if (index < offsets.length - 1) {
                    assertEquals(offsets[index + 1], messageAndOffset.offset);
                }
                final String realMessage = Utils.toString(messageAndOffset.message.payload(), "UTF-8");
                assertEquals(messages[index], realMessage);
                index++;
            }
        }
        //
        close(jafka);
    }

}
