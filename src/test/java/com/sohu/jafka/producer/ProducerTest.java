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

import java.util.Properties;

import org.junit.Test;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.producer.serializer.StringEncoder;

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
    public void testSend() {
        Properties mainProperties = new Properties();
        mainProperties.put("http.port","9093");
        Jafka jafka = createJafka(mainProperties);
        Properties producerConfig = new Properties();
        producerConfig.setProperty("broker.list", "0:localhost:"+jafka.getPort());
        producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
        for (int i = 0; i < 1000; i++) {
            producer.send(new StringProducerData("demo").add("Hello jafka").add("https://github.com/adyliu/jafka"));
        }
        producer.close();
        ////////////////////////////////////////////////
        close(jafka);
    }
    /**
     * Test method for
     * {@link com.sohu.jafka.producer.Producer#send(com.sohu.jafka.producer.ProducerData)}.
     */
    @Test
    public void testSendWithPartition() {
        Properties props = new Properties();
        props.setProperty("num.partitions", "5");
        Jafka jafka = createJafka(props);
        Properties producerConfig = new Properties();
        producerConfig.setProperty("broker.list", String.format("0:localhost:%d:5",jafka.getPort()));
        StringProducer producer = new StringProducer(new ProducerConfig(producerConfig));
        for (int i = 0; i < 1000; i++) {
            final String message = "Hello jafka. #"+i;
            StringProducerData data= new StringProducerData("demo");
            data.setKey(message);
            data.add(message).add("https://github.com/adyliu/jafka");
            producer.send(data);
        }
        producer.close();
        ////////////////////////////////////////////////
        close(jafka);
    }
}
