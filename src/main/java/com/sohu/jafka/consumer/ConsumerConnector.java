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


import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.sohu.jafka.producer.serializer.Decoder;

/**
 * Main interface for consumer
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public interface ConsumerConnector extends Closeable{

    /**
     * Create a list of {@link MessageStream} for each topic
     * 
     * @param topicCountMap a map of (topic,#streams) pair
     * @param decoder message decoder
     * @return a map of (topic,list of MessageStream) pair. The number of
     *         items in the list is #streams. Each MessageStream supports
     *         an iterator of messages.
     */
    <T> Map<String, List<MessageStream<T>>> createMessageStreams(//
            Map<String, Integer> topicCountMap, Decoder<T> decoder);

    /**
     * Commit the offsets of all broker partitions connected by this
     * connector
     */
    void commitOffsets();

    /**
     * Shut down the connector
     */
    public void close() throws IOException;
}
