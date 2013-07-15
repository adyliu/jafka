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

import java.util.ArrayList;
import java.util.List;

/**
 * string data
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class StringProducerData extends ProducerData<String, String> {


    public StringProducerData(String topic, String key, List<String> data) {
        super(topic, key, data);
    }

    public StringProducerData(String topic, List<String> data) {
        super(topic, data);
    }

    public StringProducerData(String topic, String data) {
        super(topic, data);
    }

    public StringProducerData(String topic) {
        this(topic, new ArrayList<String>());
    }

    public StringProducerData add(String message) {
        getData().add(message);
        return this;
    }
}
