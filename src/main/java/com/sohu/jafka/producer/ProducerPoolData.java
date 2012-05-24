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

import java.util.List;

import com.sohu.jafka.cluster.Partition;

/**
 * message with topic and partition
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 * @see Partition
 */
public class ProducerPoolData<V> {

    public final String topic;

    public final Partition partition;

    public final List<V> data;

    public ProducerPoolData(String topic, Partition partition, List<V> data) {
        super();
        this.topic = topic;
        this.partition = partition;
        this.data = data;
    }

    @Override
    public String toString() {
        return "ProducerPoolData [topic=" + topic + ", partition=" + partition + ", data size=" + (data != null ? data.size() : -1) + "]";
    }

}
