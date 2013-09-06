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

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import com.sohu.jafka.common.ConsumerRebalanceFailedException;
import com.sohu.jafka.server.ServerStartable;
import com.sohu.jafka.utils.zookeeper.ZkUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZookeeperTopicEventWatcher implements Closeable {

    private final TopicEventHandler<String> eventHandler;

    private final ServerStartable serverStartable;

    private final Object lock = new Object();

    private ZkClient zkClient;

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperTopicEventWatcher.class);


    public ZookeeperTopicEventWatcher(ConsumerConfig consumerConfig, TopicEventHandler<String> eventHandler, ServerStartable serverStartable) {
        super();
        this.eventHandler = eventHandler;
        this.serverStartable = serverStartable;
        //
        this.zkClient = new ZkClient(consumerConfig.getZkConnect(), //
                consumerConfig.getZkSessionTimeoutMs(), //
                consumerConfig.getZkConnectionTimeoutMs());

        startWatchingTopicEvents();

    }

    private void startWatchingTopicEvents() {

        ZkTopicEventListener topicEventListener = new ZkTopicEventListener();
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);
        zkClient.subscribeStateChanges(new ZkSessionExpireListener(topicEventListener));

        List<String> topics = zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);
        //
        // call to bootstrap topic list
        try {
            topicEventListener.handleChildChange(ZkUtils.BrokerTopicsPath, topics);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }

    private void stopWatchingTopicEvents() {
        this.zkClient.unsubscribeAll();
    }

    public void close() throws IOException {
        synchronized (lock) {
            if (zkClient == null) {
                logger.warn("cannot shutdown already shutdown topic event watcher.");
                return;
            }
            stopWatchingTopicEvents();
            zkClient.close();
            zkClient = null;
        }
    }

    class ZkTopicEventListener implements IZkChildListener {

        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            synchronized (lock) {
                if (zkClient == null)
                    return;
                try {
                    List<String> latestTopics = zkClient.getChildren(ZkUtils.BrokerTopicsPath);
                    logger.debug("all Topics: " + latestTopics);
                    eventHandler.handleTopicEvent(latestTopics);
                } catch (ConsumerRebalanceFailedException e) {
                    logger.error("can't rebalance in embedded consumer); proceed to shutdown", e);
                    serverStartable.close();
                } catch (Exception e) {
                    logger.error("error in handling child changes in embedded consumer", e);
                }
            }
        }
    }

    class ZkSessionExpireListener implements IZkStateListener {

        private final ZkTopicEventListener zkTopicEventListener;

        public ZkSessionExpireListener(ZkTopicEventListener zkTopicEventListener) {
            this.zkTopicEventListener = zkTopicEventListener;
        }

        public void handleNewSession() throws Exception {
            synchronized (lock) {
                if (zkClient != null) {
                    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, zkTopicEventListener);
                }
            }
        }

        public void handleStateChanged(KeeperState state) throws Exception {
        }
    }

}
