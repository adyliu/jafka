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

package com.sohu.jafka.producer.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


import com.sohu.jafka.api.ProducerRequest;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.CompressionCodec;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.SyncProducer;
import com.sohu.jafka.producer.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class DefaultEventHandler<T> implements EventHandler<T> {

    private final CallbackHandler<T> callbackHandler;

    private final Set<String> compressedTopics;

    private final CompressionCodec codec;

    private final Logger logger = LoggerFactory.getLogger(DefaultEventHandler.class);

    private final int numRetries;

    public DefaultEventHandler(ProducerConfig producerConfig, CallbackHandler<T> callbackHandler) {
        this.callbackHandler = callbackHandler;
        this.compressedTopics = new HashSet<String>(producerConfig.getCompressedTopics());
        this.codec = producerConfig.getCompressionCodec();
        this.numRetries = producerConfig.getNumRetries();
    }

    public void init(Properties properties) {
    }

    public void handle(List<QueueItem<T>> events, SyncProducer producer, Encoder<T> encoder) {
        List<QueueItem<T>> processedEvents = events;
        if (this.callbackHandler != null) {
            processedEvents = this.callbackHandler.beforeSendingData(events);
        }
        send(collate(processedEvents, encoder), producer);
    }

    private void send(List<ProducerRequest> produces, SyncProducer syncProducer) {
        if (produces.isEmpty()) {
            return;
        }
        final int maxAttempts = 1 + numRetries;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                syncProducer.multiSend(produces);
                break;
            } catch (RuntimeException e) {
                logger.warn("error sending message, attempts times: " + i, e);
                if (i == maxAttempts - 1) {
                    throw e;
                }
            }
        }
    }

    private List<ProducerRequest> collate(List<QueueItem<T>> events, Encoder<T> encoder) {
        if(events == null || events.isEmpty()){
            return Collections.emptyList();
        }
        final Map<String, Map<Integer, List<Message>>> topicPartitionData = new HashMap<String, Map<Integer, List<Message>>>();
        for (QueueItem<T> event : events) {
            Map<Integer, List<Message>> partitionData = topicPartitionData.get(event.topic);
            if (partitionData == null) {
                partitionData = new HashMap<Integer, List<Message>>();
                topicPartitionData.put(event.topic, partitionData);
            }
            List<Message> data = partitionData.get(event.partition);
            if (data == null) {
                data = new ArrayList<Message>();
                partitionData.put(event.partition, data);
            }
            data.add(encoder.toMessage(event.data));
        }
        //
        final List<ProducerRequest> requests = new ArrayList<ProducerRequest>();
        for (Map.Entry<String, Map<Integer, List<Message>>> e : topicPartitionData.entrySet()) {
            final String topic = e.getKey();
            for (Map.Entry<Integer, List<Message>> pd : e.getValue().entrySet()) {
                final Integer partition = pd.getKey();
                requests.add(new ProducerRequest(topic, partition, convert(topic, pd.getValue())));
            }
        }
        return requests;
    }

    private ByteBufferMessageSet convert(String topic, List<Message> messages) {
        //compress condition:
        if (codec != CompressionCodec.NoCompressionCodec//
                && (compressedTopics.isEmpty() || compressedTopics.contains(topic))) {
            return new ByteBufferMessageSet(codec, messages.toArray(new Message[messages.size()]));
        }
        return new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messages.toArray(new Message[messages
                .size()]));
    }

    public void close() {
    }

}
