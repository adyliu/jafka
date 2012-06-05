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

import java.io.Closeable;

import com.sohu.jafka.common.InvalidPartitionException;
import com.sohu.jafka.common.NoBrokersForPartitionException;
import com.sohu.jafka.producer.serializer.Encoder;

/**
 * Producer interface
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
public interface IProducer<K, V> extends Closeable {

    /**
     * Send messages
     * 
     * @param data message data
     * @throws NoBrokersForPartitionException no broker has this topic
     * @throws InvalidPartitionException partition is out of range
     */
    void send(ProducerData<K, V> data) throws NoBrokersForPartitionException, InvalidPartitionException;

    /**
     * get message encoder
     * 
     * @return message encoder
     * @see Encoder
     */
    Encoder<V> getEncoder();

    /**
     * get partition chooser
     * 
     * @return partition chooser
     * @see Partitioner
     */
    Partitioner<K> getPartitioner();
}
