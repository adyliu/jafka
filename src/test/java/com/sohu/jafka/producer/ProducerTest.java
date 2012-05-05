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
        Jafka jafka = createJafka();
        Properties producerConfig = new Properties();
        producerConfig.setProperty("broker.list", "0:localhost:9092");
        producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
        producer.send(new StringProducerData("demo").add("Hello jafka").add("https://github.com/adyliu/jafka"));
        producer.close();
        close(jafka);
    }

}
