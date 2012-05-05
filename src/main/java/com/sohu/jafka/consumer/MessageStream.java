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


import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

import com.sohu.jafka.common.annotations.ClientSide;
import com.sohu.jafka.producer.serializer.Decoder;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
public class MessageStream<T> implements Iterable<T> {

    final String topic;

    final BlockingQueue<FetchedDataChunk> queue;

    final int consumerTimeoutMs;

    final Decoder<T> decoder;

    private final ConsumerIterator<T> consumerIterator;

    public MessageStream(String topic, BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs, Decoder<T> decoder) {
        super();
        this.topic = topic;
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.decoder = decoder;
        this.consumerIterator = new ConsumerIterator<T>(topic, queue, consumerTimeoutMs, decoder);
    }

    public Iterator<T> iterator() {
        return consumerIterator;
    }

    /**
     * This method clears the queue being iterated during the consumer
     * rebalancing. This is mainly to reduce the number of duplicates
     * received by the consumer
     */
    public void clear() {
        consumerIterator.clearCurrentChunk();
    }
}
