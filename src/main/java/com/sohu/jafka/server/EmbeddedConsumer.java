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

package com.sohu.jafka.server;

import com.sohu.jafka.consumer.Consumer;
import com.sohu.jafka.consumer.ConsumerConfig;
import com.sohu.jafka.consumer.ConsumerConnector;
import com.sohu.jafka.consumer.MessageStream;
import com.sohu.jafka.consumer.TopicEventHandler;
import com.sohu.jafka.consumer.ZookeeperTopicEventWatcher;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.serializer.MessageEncoders;
import com.sohu.jafka.utils.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class EmbeddedConsumer implements TopicEventHandler<String> {

    private final ConsumerConfig consumerConfig;

    private final ServerStartable serverStartable;

    //
    private final List<String> whiteListTopics;

    private final List<String> blackListTopics;

    private final Producer<Void, Message> producer;

    //
    private ZookeeperTopicEventWatcher topicEventWatcher;

    private ConsumerConnector consumerConnector;

    private List<MirroringThread> threadList = new ArrayList<MirroringThread>();

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedConsumer.class);

    private List<String> mirrorTopics = new ArrayList<String>();

    /**
     * @param consumerConfig  the inner consumer config
     * @param producerConfig  the producer config
     * @param serverStartable server instance
     */
    public EmbeddedConsumer(ConsumerConfig consumerConfig, ProducerConfig producerConfig, ServerStartable serverStartable) {
        this.consumerConfig = consumerConfig;
        this.serverStartable = serverStartable;
        //
        this.whiteListTopics = Arrays.asList(consumerConfig.getMirrorTopicsWhitelist().split(","));
        this.blackListTopics = Arrays.asList(consumerConfig.getMirrorTopicsWhitelist().split(","));
        this.producer = new Producer<Void, Message>(producerConfig);
    }

    /**
     *
     */
    public void startup() {
        logger.info("staring up embedded consumer");
        topicEventWatcher = new ZookeeperTopicEventWatcher(consumerConfig, this, serverStartable);
    }

    /**
     *
     */
    public void shutdown() {
        if (topicEventWatcher != null) {
            Closer.closeQuietly(topicEventWatcher);
        }
        if (consumerConnector != null) {
            Closer.closeQuietly(consumerConnector);
        }
        for (MirroringThread thread : threadList) {
            thread.close();
        }
        producer.close();
    }

    public void handleTopicEvent(List<String> allTopics) {
        List<String> newMirrorTopics = new ArrayList<String>();
        List<String> addedTopics = new ArrayList<String>();
        List<String> deletedTopics = new ArrayList<String>();
        final Map<String, Integer> topicCountMap = new LinkedHashMap<String, Integer>();
        for (String topic : allTopics) {
            boolean newTopic = false;
            if (whiteListTopics.isEmpty()) {
                newTopic = whiteListTopics.contains(topic);
            } else {
                newTopic = !blackListTopics.contains(topic);
            }
            if (newTopic) {
                newMirrorTopics.add(topic);
                topicCountMap.put(topic, consumerConfig.getMirrorConsumerNumThreads());
                if (!mirrorTopics.contains(topic)) {
                    addedTopics.add(topic);
                }
            }
        }
        //
        //
        for (String topic : mirrorTopics) {
            if (!newMirrorTopics.contains(topic)) {
                deletedTopics.add(topic);
            }
        }
        //
        mirrorTopics = newMirrorTopics;
        //
        if (!addedTopics.isEmpty() || !deletedTopics.isEmpty()) {
            startNewConsumerThreads(topicCountMap);
        }
    }

    private void startNewConsumerThreads(Map<String, Integer> topicCountMap) {
        if (topicCountMap.isEmpty()) return;
        if (consumerConnector != null) {
            Closer.closeQuietly(consumerConnector);
        }
        for (MirroringThread thread : threadList) {
            thread.close();
        }

        threadList.clear();

        consumerConnector = Consumer.create(consumerConfig);
        Map<String, List<MessageStream<Message>>> streams = consumerConnector.createMessageStreams(topicCountMap, new MessageEncoders());
        for (Map.Entry<String, List<MessageStream<Message>>> e : streams.entrySet()) {
            int i = 0;
            for (MessageStream<Message> stream : e.getValue()) {
                threadList.add(new MirroringThread(stream, e.getKey(), i++, producer));
            }
        }
        for (MirroringThread t : threadList) {
            t.start();
        }
    }

}
