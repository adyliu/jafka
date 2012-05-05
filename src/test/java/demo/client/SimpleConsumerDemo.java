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


import java.io.IOException;

import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.consumer.SimpleConsumer;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SimpleConsumerDemo {

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {

        SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092, 10000, 1024000);
        //
        long offset = 0;
        int cnt = 0;
        while (true) {
            //
            FetchRequest request = new FetchRequest("test", 0, offset, 100000);
            for (MessageAndOffset msg : consumer.fetch(request)) {
                cnt++;
                System.out.println(String.format("[%5d] %10d consumed: %s", //
                        cnt,//
                        msg.offset,//
                        Utils.toString(msg.message.payload(), "UTF-8")));
                //
                offset = msg.offset;
            }
        }
    }

}
