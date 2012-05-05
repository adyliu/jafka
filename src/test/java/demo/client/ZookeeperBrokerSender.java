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

package demo.client;

import java.util.Properties;

import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringEncoder;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZookeeperBrokerSender {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("zk.connect", "zk1.in.i.sohu.com:2181,zk2.in.i.sohu.com:2181,zk3.in.i.sohu.com:2181,zk4.in.i.sohu.com:2181,zk5.in.i.sohu.com:2181/talent/suc/jafka");
        props.put("serializer.class", StringEncoder.class.getName());
        //
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        //
        StringProducerData data = new StringProducerData("demo");
        for(int i=0;i<100;i++) {
            data.add("Hello world #"+i);
        }
        //
        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                producer.send(data);
            }
            long cost = System.currentTimeMillis() - start;
            System.out.println("send 10000 message cost: "+cost+" ms");
        } finally {
            producer.close();
        }
    }

}
