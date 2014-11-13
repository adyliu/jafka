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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import com.sohu.jafka.cluster.Broker;
import com.sohu.jafka.cluster.Partition;
import com.sohu.jafka.common.NoBrokersForPartitionException;
import com.sohu.jafka.utils.ZKConfig;
import com.sohu.jafka.utils.zookeeper.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZKBrokerPartitionInfo implements BrokerPartitionInfo {

    private final Logger logger = LoggerFactory.getLogger(ZKBrokerPartitionInfo.class);

    final ZKConfig zkConfig;

    final Callback callback;

    private final Object zkWatcherLock = new Object();

    private final ZkClient zkClient;

    private Map<String, SortedSet<Partition>> topicBrokerPartitions;

    private Map<Integer, Broker> allBrokers;

    private BrokerTopicsListener brokerTopicsListener;

    public ZKBrokerPartitionInfo(ZKConfig zkConfig, Callback callback) {
        this.zkConfig = zkConfig;
        this.callback = callback;
        //
        this.zkClient = new ZkClient(zkConfig.getZkConnect(), //
                zkConfig.getZkSessionTimeoutMs(), //
                zkConfig.getZkConnectionTimeoutMs());
        //
        this.allBrokers = getZKBrokerInfo();
        this.topicBrokerPartitions = getZKTopicPartitionInfo(this.allBrokers);
        //use just the brokerTopicsListener for all watchers
        this.brokerTopicsListener = new BrokerTopicsListener(this.topicBrokerPartitions, this.allBrokers);

        //register listener for change of topics to keep topicsBrokerPartitions updated
        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, brokerTopicsListener);

        //register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
        for (String topic : this.topicBrokerPartitions.keySet()) {
            zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, this.brokerTopicsListener);
        }
        // register listener for new broker
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, this.brokerTopicsListener);
        //
        // register listener for session expired event
        zkClient.subscribeStateChanges(new ZKSessionExpirationListener());
    }

    public SortedSet<Partition> getBrokerPartitionInfo(final String topic) {
        synchronized (zkWatcherLock) {
            SortedSet<Partition> brokerPartitions = topicBrokerPartitions.get(topic);
            if (brokerPartitions == null || brokerPartitions.size() == 0) {
                brokerPartitions = bootstrapWithExistingBrokers(topic);
                topicBrokerPartitions.put(topic, brokerPartitions);
                return brokerPartitions;
            } else {
                return new TreeSet<Partition>(brokerPartitions);
            }
        }
    }
    
    private SortedSet<Partition> bootstrapWithExistingBrokers(String topic) {
        List<String> brokers = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        if (brokers == null) {
            throw new NoBrokersForPartitionException("no brokers");
        }
        TreeSet<Partition> partitions = new TreeSet<Partition>();
        for (String brokerId : brokers) {
            partitions.add(new Partition(Integer.valueOf(brokerId), 0));
        }
        return partitions;
    }

    public Broker getBrokerInfo(int brokerId) {
        synchronized (zkWatcherLock) {
            return allBrokers.get(brokerId);
        }
    }

    public Map<Integer, Broker> getAllBrokerInfo() {
        return allBrokers;
    }

    /**
     * Generate a sequence of (brokerId, numPartitions) for all topics registered in zookeeper
     * @param allBrokers all register brokers
     * @return a mapping from topic to sequence of (brokerId, numPartitions)
     */
    private Map<String, SortedSet<Partition>> getZKTopicPartitionInfo(Map<Integer, Broker> allBrokers) {
        final Map<String, SortedSet<Partition>> brokerPartitionsPerTopic = new HashMap<String, SortedSet<Partition>>();
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath);
        for (String topic : topics) {
            // find the number of broker partitions registered for this topic
            String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
            List<String> brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, brokerTopicPath);
            //
            final SortedSet<Partition> sortedBrokerPartitions = new TreeSet<Partition>();
            final Set<Integer> existBids = new HashSet<Integer>();
            for (String bid : brokerList) {
                final int ibid = Integer.parseInt(bid);
                final String numPath = brokerTopicPath + "/" + bid;
                final Integer numPartition = Integer.valueOf(ZkUtils.readData(zkClient, numPath));
                for (int i = 0; i < numPartition.intValue(); i++) {
                    sortedBrokerPartitions.add(new Partition(ibid, i));
                }
                existBids.add(ibid);
            }
            // add all brokers after topic created
            for(Integer bid:allBrokers.keySet()){
                if(!existBids.contains(bid)){
                    sortedBrokerPartitions.add(new Partition(bid,0));// this broker run after topic created
                }
            }
            logger.debug("Broker ids and # of partitions on each for topic: " + topic + " = " + sortedBrokerPartitions);
            brokerPartitionsPerTopic.put(topic, sortedBrokerPartitions);
        }
        return brokerPartitionsPerTopic;
    }

    private Map<Integer, Broker> getZKBrokerInfo() {
        Map<Integer, Broker> brokers = new HashMap<Integer, Broker>();
        List<String> allBrokersIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        if (allBrokersIds != null) {
            logger.info("read all brokers count: " + allBrokersIds.size());
            for (String brokerId : allBrokersIds) {
                String brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId);
                Broker createBroker = Broker.createBroker(Integer.valueOf(brokerId), brokerInfo);
                brokers.put(Integer.valueOf(brokerId), createBroker);
                logger.info("Loading Broker " + createBroker);
            }
        }
        return brokers;
    }

    public void updateInfo() {
        synchronized (this.zkWatcherLock) {
            this.allBrokers = getZKBrokerInfo();
            this.topicBrokerPartitions = getZKTopicPartitionInfo(this.allBrokers);
        }
    }

    public void close() {
        this.zkClient.close();
    }

    class BrokerTopicsListener implements IZkChildListener {

        private Map<String, SortedSet<Partition>> originalBrokerTopicsParitions;

        private Map<Integer, Broker> originBrokerIds;

        public BrokerTopicsListener(Map<String, SortedSet<Partition>> originalBrokerTopicsParitions,
                Map<Integer, Broker> originBrokerIds) {
            super();
            this.originalBrokerTopicsParitions = new LinkedHashMap<String, SortedSet<Partition>>(
                    originalBrokerTopicsParitions);
            this.originBrokerIds = new LinkedHashMap<Integer, Broker>(originBrokerIds);
            logger.debug("[BrokerTopicsListener] Creating broker topics listener to watch the following paths - \n" + "/broker/topics, /broker/topics/<topic>, /broker/<ids>");
            logger.debug("[BrokerTopicsListener] Initialized this broker topics listener with initial mapping of broker id to " + "partition id per topic with " + originalBrokerTopicsParitions);
        }

        public void handleChildChange(final String parentPath, List<String> currentChilds) throws Exception {
            final List<String> curChilds = currentChilds != null ? currentChilds : new ArrayList<String>();
            synchronized (zkWatcherLock) {
                if (ZkUtils.BrokerTopicsPath.equals(parentPath)) {
                    Iterator<String> updatedTopics = curChilds.iterator();
                    while (updatedTopics.hasNext()) {
                        String t = updatedTopics.next();
                        if (originalBrokerTopicsParitions.containsKey(t)) {
                            updatedTopics.remove();
                        }
                    }
                    for (String addedTopic : curChilds) {
                        String path = ZkUtils.BrokerTopicsPath + "/" + addedTopic;
                        List<String> brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, path);
                        processNewBrokerInExistingTopic(addedTopic, brokerList);
                        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + addedTopic,
                                brokerTopicsListener);
                    }
                } else if (ZkUtils.BrokerIdsPath.equals(parentPath)) {
                    processBrokerChange(parentPath, curChilds);
                } else {
                    //check path: /brokers/topics/<topicname>
                    String[] ps = parentPath.split("/");
                    if (ps.length == 4 && "topics".equals(ps[2])) {
                        logger.debug("[BrokerTopicsListener] List of brokers changed at " + parentPath + "\t Currently registered " + " list of brokers -> " + curChilds + " for topic -> " + ps[3]);
                        processNewBrokerInExistingTopic(ps[3], curChilds);
                    }
                }
                //
                //update the data structures tracking older state values
                resetState();
            }
        }


        private void processBrokerChange(String parentPath, List<String> curChilds) {
            final Map<Integer, Broker> oldBrokerIdMap = new HashMap<Integer, Broker>(originBrokerIds);
            for (int i = curChilds.size() - 1; i >= 0; i--) {
                Integer brokerId = Integer.valueOf(curChilds.get(i));
                if (oldBrokerIdMap.remove(brokerId) != null) {//old topic
                    curChilds.remove(i);//remove old topics and left new topics
                }
            }
            //now curChilds are all new brokers
            //oldBrokerIdMap are all dead brokers
            for (String newBroker : curChilds) {
                final String brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + newBroker);
                final Integer newBrokerId = Integer.valueOf(newBroker);
                final Broker broker = Broker.createBroker(newBrokerId.intValue(),brokerInfo);
                allBrokers.put(newBrokerId, broker);
                callback.producerCbk(broker.id, broker.host, broker.port,broker.autocreated);
            }
            //
            //remove all dead broker and remove all broker-partition from topic list
            for (Map.Entry<Integer, Broker> deadBroker : oldBrokerIdMap.entrySet()) {
                //remove dead broker
                allBrokers.remove(deadBroker.getKey());

                //remove dead broker-partition from topic
                for (Map.Entry<String, SortedSet<Partition>> topicParition : topicBrokerPartitions.entrySet()) {
                    Iterator<Partition> partitions = topicParition.getValue().iterator();
                    while (partitions.hasNext()) {
                        Partition p = partitions.next();
                        if (deadBroker.getKey().intValue() == p.brokerId) {
                            partitions.remove();
                        }
                    }
                }
            }
        }

        private void processNewBrokerInExistingTopic(String topic, List<String> brokerList) {

            SortedSet<Partition> updatedBrokerParts = getBrokerPartitions(zkClient, topic, brokerList);
            SortedSet<Partition> oldBrokerParts = topicBrokerPartitions.get(topic);
            SortedSet<Partition> mergedBrokerParts = new TreeSet<Partition>();
            if (oldBrokerParts != null) {
                mergedBrokerParts.addAll(oldBrokerParts);
            }
            //override old parts or add new parts
            mergedBrokerParts.addAll(updatedBrokerParts);
            //
            // keep only brokers that are alive
            Iterator<Partition> iter = mergedBrokerParts.iterator();
            while (iter.hasNext()) {
                if (!allBrokers.containsKey(iter.next().brokerId)) {
                    iter.remove();
                }
            }
            //            mergedBrokerParts = Sets.filter(mergedBrokerParts, new Predicate<Partition>() {
            //
            //                public boolean apply(Partition input) {
            //                    return allBrokers.containsKey(input.brokerId);
            //                }
            //            });
            topicBrokerPartitions.put(topic, mergedBrokerParts);
            logger.debug("[BrokerTopicsListener] List of broker partitions for topic: " + topic + " are " + mergedBrokerParts);
        }

        private void resetState() {
            logger.debug("[BrokerTopicsListener] Before reseting broker topic partitions state " + this.originalBrokerTopicsParitions);
            this.originalBrokerTopicsParitions = new HashMap<String, SortedSet<Partition>>(topicBrokerPartitions);
            logger.debug("[BrokerTopicsListener] After reseting broker topic partitions state " + originalBrokerTopicsParitions);
            //
            logger.debug("[BrokerTopicsListener] Before reseting broker id map state " + originBrokerIds);
            this.originBrokerIds = new HashMap<Integer, Broker>(allBrokers);
            logger.debug("[BrokerTopicsListener] After reseting broker id map state " + originBrokerIds);
        }
    }

    class ZKSessionExpirationListener implements IZkStateListener {

        public void handleNewSession() throws Exception {
            /**
             * When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has
             * reestablished a connection for us.
             */
            logger.info("ZK expired; release old list of broker partitions for topics ");
            allBrokers = getZKBrokerInfo();
            topicBrokerPartitions = getZKTopicPartitionInfo(allBrokers);
            brokerTopicsListener.resetState();

            // register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
            // NOTE: this is probably not required here. Since when we read from getZKTopicPartitionInfo() above,
            // it automatically recreates the watchers there itself
            for (String topic : topicBrokerPartitions.keySet()) {
                zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, brokerTopicsListener);
            }
            // there is no need to re-register other listeners as they are listening on the child changes of
            // permanent nodes
        }

        public void handleStateChanged(KeeperState state) throws Exception {
        }
    }

    /**
     * Generate a mapping from broker id to (brokerId, numPartitions) for the list of brokers
     * specified
     * 
     * @param topic the topic to which the brokers have registered
     * @param brokerList the list of brokers for which the partitions info is to be generated
     * @return a sequence of (brokerId, numPartitions) for brokers in brokerList
     */
    private static SortedSet<Partition> getBrokerPartitions(ZkClient zkClient, String topic, List<?> brokerList) {
        final String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
        final SortedSet<Partition> brokerParts = new TreeSet<Partition>();
        for (Object brokerId : brokerList) {
            final Integer bid = Integer.valueOf(brokerId.toString());
            final Integer numPartition = Integer.valueOf(ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid));
            for (int i = 0; i < numPartition.intValue(); i++) {
                brokerParts.add(new Partition(bid, i));
            }
        }
        return brokerParts;
    }
}
