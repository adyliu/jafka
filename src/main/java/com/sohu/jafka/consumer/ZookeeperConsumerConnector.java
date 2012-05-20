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

import static java.lang.String.format;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNodeExistsException;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.cluster.Broker;
import com.sohu.jafka.cluster.Cluster;
import com.sohu.jafka.cluster.Partition;
import com.sohu.jafka.common.ConsumerRebalanceFailedException;
import com.sohu.jafka.common.InvalidConfigException;
import com.sohu.jafka.producer.serializer.Decoder;
import com.sohu.jafka.utils.Closer;
import com.sohu.jafka.utils.KV.StringTuple;
import com.sohu.jafka.utils.Pool;
import com.sohu.jafka.utils.Scheduler;
import com.sohu.jafka.utils.zookeeper.ZKStringSerializer;
import com.sohu.jafka.utils.zookeeper.ZkGroupDirs;
import com.sohu.jafka.utils.zookeeper.ZkGroupTopicDirs;
import com.sohu.jafka.utils.zookeeper.ZkUtils;

/**
 * This class handles the consumers interaction with zookeeper
 * 
 * Directories:
 * <p>
 * <b>1. Consumer id registry:</b>
 * 
 * <pre>
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * </pre>
 * 
 * A consumer has a unique consumer id within a consumer group. A consumer
 * registers its id as an ephemeral znode and puts all topics that it
 * subscribes to as the value of the znode. The znode is deleted when the
 * client is gone. A consumer subscribes to event changes of the consumer
 * id registry within its group.
 * </p>
 * <p>
 * The consumer id is picked up from configuration, instead of the
 * sequential id assigned by ZK. Generated sequential ids are hard to
 * recover during temporary connection loss to ZK, since it's difficult for
 * the client to figure out whether the creation of a sequential znode has
 * succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 * </p>
 * <p>
 * <b>2. Broker node registry:</b>
 * 
 * <pre>
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * </pre>
 * 
 * This is a list of all present broker brokers. A unique logical node id
 * is configured on each broker node. A broker node registers itself on
 * start-up and creates a znode with the logical node id under /brokers.
 * 
 * The value of the znode is a JSON String that contains
 * 
 * <pre>
 * (1) the host name and the port the broker is listening to, 
 * (2) a list of topics that the broker serves, 
 * (3) a list of logical partitions assigned to each topic on the broker.
 * </pre>
 * 
 * A consumer subscribes to event changes of the broker node registry.
 * </p>
 * 
 * <p>
 * <b>3. Partition owner registry:</b>
 * 
 * <pre>
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * </pre>
 * 
 * This stores the mapping before broker partitions and consumers. Each
 * partition is owned by a unique consumer within a consumer group. The
 * mapping is reestablished after each rebalancing.
 * </p>
 * 
 * <p>
 * <b>4. Consumer offset tracking:</b>
 * 
 * <pre>
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * </pre>
 * 
 * Each consumer tracks the offset of the latest message consumed for each
 * partition.
 * </p>
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZookeeperConsumerConnector implements ConsumerConnector {

    public static final FetchedDataChunk SHUTDOWN_COMMAND = new FetchedDataChunk(null, null, -1);

    private final Logger logger = Logger.getLogger(ZookeeperConsumerConnector.class);

    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    private final Object rebalanceLock = new Object();

    private Fetcher fetcher;

    private ZkClient zkClient;

    private Pool<String, Pool<Partition, PartitionTopicInfo>> topicRegistry;

    //
    private final Pool<StringTuple, BlockingQueue<FetchedDataChunk>> queues;

    private final Scheduler scheduler = new Scheduler(1, "consumer-autocommit-", false);

    final ConsumerConfig config;

    final boolean enableFetcher;

    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }

    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
        this.config = config;
        this.enableFetcher = enableFetcher;
        //
        this.topicRegistry = new Pool<String, Pool<Partition, PartitionTopicInfo>>();
        this.queues = new Pool<StringTuple, BlockingQueue<FetchedDataChunk>>();
        //
        connectZk();
        createFetcher();
        if (this.config.isAutoCommit()) {
            logger.info("starting auto committer every " + config.getAutoCommitIntervalMs() + " ms");
            scheduler.scheduleWithRate(new AutoCommitTask(), config.getAutoCommitIntervalMs(), config.getAutoCommitIntervalMs());
        }
    }

    /**
     * 
     */
    private void createFetcher() {
        if (enableFetcher) {
            this.fetcher = new Fetcher(config, zkClient);
        }
    }

    class AutoCommitTask implements Runnable {

        public void run() {
            try {
                commitOffsets();
            } catch (Throwable e) {
                logger.error("exception during autoCommit: ", e);
            }
        }
    }

    public <T> Map<String, List<MessageStream<T>>> createMessageStreams(Map<String, Integer> topicCountMap, Decoder<T> decoder) {
        return consume(topicCountMap, decoder);
    }

    private <T> Map<String, List<MessageStream<T>>> consume(Map<String, Integer> topicCountMap, Decoder<T> decoder) {
        if (topicCountMap == null) {
            throw new IllegalArgumentException("topicCountMap is null");
        }
        //
        ZkGroupDirs dirs = new ZkGroupDirs(config.getGroupId());
        Map<String, List<MessageStream<T>>> ret = new HashMap<String, List<MessageStream<T>>>();
        String consumerUuid = config.getConsumerId();
        if (consumerUuid == null) {
            consumerUuid = generateConsumerId();
        }
        logger.info(format("create message stream by consumerid [%s] with groupid [%s]",consumerUuid,config.getGroupId()));
        //
        //consumerIdString => groupid_consumerid
        final String consumerIdString = config.getGroupId() + "_" + consumerUuid;
        final TopicCount topicCount = new TopicCount(consumerIdString, topicCountMap);
        for (Map.Entry<String, Set<String>> e : topicCount.getConsumerThreadIdsPerTopic().entrySet()) {
            final String topic = e.getKey();
            final Set<String> threadIdSet = e.getValue();
            final List<MessageStream<T>> streamList = new ArrayList<MessageStream<T>>();
            for (String threadId : threadIdSet) {
                LinkedBlockingQueue<FetchedDataChunk> stream = new LinkedBlockingQueue<FetchedDataChunk>(config.getMaxQueuedChunks());
                queues.put(new StringTuple(topic, threadId), stream);
                streamList.add(new MessageStream<T>(topic, stream, config.getConsumerTimeoutMs(), decoder));
            }
            ret.put(topic, streamList);
            logger.debug("adding topic " + topic + " and stream to map.");
        }
        //
        //listener to consumer and partition changes
        ZKRebalancerListener<T> loadBalancerListener = new ZKRebalancerListener<T>(config.getGroupId(), consumerIdString, ret);
        loadBalancerListener.start();
        registerConsumerInZK(dirs, consumerIdString, topicCount);
        //
        //register listener for session expired event
        zkClient.subscribeStateChanges(new ZKSessionExpireListener<T>(dirs, consumerIdString, topicCount, loadBalancerListener));
        zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener);
        //
        for (String topic : ret.keySet()) {
            //register on broker partition path changes
            final String partitionPath = ZkUtils.BrokerTopicsPath + "/" + topic;
            zkClient.subscribeChildChanges(partitionPath, loadBalancerListener);
        }

        //explicitly grigger load balancing for this consumer
        loadBalancerListener.syncedRebalance();
        return ret;
    }
    /**
     * generate random consumerid ( hostname-currenttime-uuid.sub(8) )
     * @return random consumerid
     */
    private String generateConsumerId() {
        UUID uuid = UUID.randomUUID();
        try {
            return format("%s-%d-%s", InetAddress.getLocalHost().getHostName(), //
                    System.currentTimeMillis(),//
                    Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("can not generate consume id by auto, set the 'consumerid' parameter to fix this");
        }
    }

    public void commitOffsets() {
        if (zkClient == null) {
            logger.error("zk client is null. Cannot commit offsets");
            return;
        }
        for (Entry<String, Pool<Partition, PartitionTopicInfo>> e : topicRegistry.entrySet()) {
            ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(config.getGroupId(), e.getKey());
            //
            for (PartitionTopicInfo info : e.getValue().values()) {
                final long lastChanged = info.getConsumedOffsetChanged().get();
                if (lastChanged == 0) {
                    logger.debug("consume offset not changed");
                    continue;
                }
                final long newOffset = info.getConsumedOffset();
                //path: /consumers/<group>/offsets/<topic>/<brokerid-partition>
                final String path = topicDirs.consumerOffsetDir + "/" + info.partition.getName();
                try {
                    ZkUtils.updatePersistentPath(zkClient, path, "" + newOffset);
                } catch (Throwable t) {
                    logger.warn("exception during commitOffsets, path=" + path + ",offset=" + newOffset, t);
                } finally {
                    info.resetComsumedOffsetChanged(lastChanged);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Committed [" + path + "] for topic " + info);
                    }
                }
            }
            //
        }
    }

    public void close() throws IOException {
        if (isShuttingDown.compareAndSet(false, true)) {
            logger.info("ZkConsumerConnector shutting down");
            try {
                scheduler.shutdown();
                if (fetcher != null) {
                    fetcher.stopConnectionsToAllBrokers();
                }
                sendShutdownToAllQueues();
                if (config.isAutoCommit()) {
                    commitOffsets();
                }
                if (this.zkClient != null) {
                    this.zkClient.close();
                    zkClient = null;
                }
            } catch (Exception e) {
                logger.fatal("error during consumer connector shutdown", e);
            }
            logger.info("ZkConsumerConnector shut down completed");
        }
    }

    /**
     * 
     */
    private void sendShutdownToAllQueues() {
        for (BlockingQueue<FetchedDataChunk> queue : queues.values()) {
            queue.clear();
            try {
                queue.put(SHUTDOWN_COMMAND);
            } catch (InterruptedException e) {
                logger.warn(e.getMessage());
            }
        }
    }

    private void connectZk() {
        logger.info("Connecting to zookeeper instance at " + config.getZkConnect());
        this.zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(), config.getZkConnectionTimeoutMs(), ZKStringSerializer.getInstance());
        logger.info("Connected to zookeeper at " + config.getZkConnect());
    }

    class ZKRebalancerListener<T> implements IZkChildListener, Runnable {

        final String group;

        final String consumerIdString;

        Map<String, List<MessageStream<T>>> messagesStreams;

        //
        private boolean isWatcherTriggered = false;

        private final ReentrantLock lock = new ReentrantLock();

        private final Condition cond = lock.newCondition();

        private final Thread watcherExecutorThread;

        public ZKRebalancerListener(String group, String consumerIdString, Map<String, List<MessageStream<T>>> messagesStreams) {
            super();
            this.group = group;
            this.consumerIdString = consumerIdString;
            this.messagesStreams = messagesStreams;
            //
            this.watcherExecutorThread = new Thread(this, consumerIdString + "_watcher_executor");
        }

        public void start() {
            this.watcherExecutorThread.start();
        }

        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            lock.lock();
            try {
                isWatcherTriggered = true;
                cond.signalAll();
            } finally {
                lock.unlock();
            }
        }

        public void run() {
            logger.info("starting watcher executor thread for consumer " + consumerIdString);
            boolean doRebalance = false;
            while (!isShuttingDown.get()) {
                try {
                    lock.lock();
                    try {
                        if (!isWatcherTriggered) {
                            cond.await(1000, TimeUnit.MILLISECONDS);//wake up periodically so that it can check the shutdown flag
                        }
                    } finally {
                        doRebalance = isWatcherTriggered;
                        isWatcherTriggered = false;
                        lock.unlock();
                    }
                    if (doRebalance) {
                        syncedRebalance();
                    }
                } catch (Throwable t) {
                    logger.error("error during syncedRebalance", t);
                }
            }
            //
            logger.info("stopping watcher executor thread for consumer " + consumerIdString);
        }

        public void syncedRebalance() {
            synchronized (rebalanceLock) {
                for (int i = 0; i < config.getMaxRebalanceRetries(); i++) {
                    logger.info("begin rebalancing consumer " + consumerIdString + " try #" + i);
                    boolean done = false;
                    Cluster cluster = ZkUtils.getCluster(zkClient);
                    try {
                        done = rebalance(cluster);
                    } catch (Exception e) {
                        /**
                         * occasionally, we may hit a ZK exception because
                         * the ZK state is changing while we are iterating.
                         * For example, a ZK node can disappear between the
                         * time we get all children and the time we try to
                         * get the value of a child. Just let this go since
                         * another rebalance will be triggered.
                         **/
                        logger.info("exception during rebalance ", e);
                    }
                    logger.info("end rebalancing consumer " + consumerIdString + " try #" + i);
                    //
                    if (done) {
                        return;
                    } else {
                        /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
                         * clear the cache */
                        logger.info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered");
                    }
                    //
                    closeFetchersForQueues(cluster, messagesStreams, queues.values());
                    try {
                        Thread.sleep(config.getRebalanceBackoffMs());
                    } catch (InterruptedException e) {
                        logger.warn(e.getMessage());
                    }
                }
            }
            throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.getMaxRebalanceRetries() + " retries");
        }

        private boolean rebalance(Cluster cluster) {
            // map for current consumer: topic->[groupid-consumer-0,groupid-consumer-1,...,groupid-consumer-N]
            Map<String, Set<String>> myTopicThreadIdsMap = ZkUtils.getTopicCount(zkClient, group, consumerIdString).getConsumerThreadIdsPerTopic();
            // map for all consumers in this group: topic->[groupid-consumer1-0,...,groupid-consumerX-N]
            Map<String, List<String>> consumersPerTopicMap = ZkUtils.getConsumersPerTopic(zkClient, group);
            // map for all broker-partitions for the topics in this consumerid: topic->[brokerid0-partition0,...,brokeridN-partitionN]
            Map<String, List<String>> brokerPartitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient, myTopicThreadIdsMap.keySet());
            /**
             * fetchers must be stopped to avoid data duplication, since if
             * the current rebalancing attempt fails, the partitions that
             * are released could be owned by another consumer. But if we
             * don't stop the fetchers first, this consumer would continue
             * returning data for released partitions in parallel. So, not
             * stopping the fetchers leads to duplicate data.
             */
            closeFetchers(cluster, messagesStreams, myTopicThreadIdsMap);
            releasePartitionOwnership(topicRegistry);
            //
            Map<StringTuple, String> partitionOwnershipDecision = new HashMap<StringTuple, String>();
            Pool<String, Pool<Partition, PartitionTopicInfo>> currentTopicRegistry = new Pool<String, Pool<Partition, PartitionTopicInfo>>();
            for (Map.Entry<String, Set<String>> e : myTopicThreadIdsMap.entrySet()) {
                final String topic = e.getKey();
                currentTopicRegistry.put(topic, new Pool<Partition, PartitionTopicInfo>());
                //
                ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
                List<String> curConsumers = consumersPerTopicMap.get(topic);
                List<String> curBrokerPartitions = brokerPartitionsPerTopicMap.get(topic);

                final int nPartsPerConsumer = curBrokerPartitions.size() / curConsumers.size();
                final int nConsumersWithExtraPart = curBrokerPartitions.size() % curConsumers.size();

                logger.info("Consumer " + consumerIdString + " rebalancing the following partitions: " + curBrokerPartitions + " for topic " + topic
                        + " with consumers: " + curConsumers);

                //consumerThreadId=> groupid_consumerid-index (index from count)
                for (String consumerThreadId : e.getValue()) {
                    final int myConsumerPosition = curConsumers.indexOf(consumerThreadId);
                    assert (myConsumerPosition >= 0);
                    final int startPart = nPartsPerConsumer * myConsumerPosition + Math.min(myConsumerPosition, nConsumersWithExtraPart);
                    final int nParts = nPartsPerConsumer + ((myConsumerPosition + 1 > nConsumersWithExtraPart) ? 0 : 1);

                    /**
                     * Range-partition the sorted partitions to consumers
                     * for better locality. The first few consumers pick up
                     * an extra partition, if any.
                     */
                    if (nParts <= 0) {
                        logger.warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic);
                    } else {
                        for (int i = startPart; i < startPart + nParts; i++) {
                            String brokerPartition = curBrokerPartitions.get(i);
                            logger.info(consumerThreadId + " attempting to claim partition " + brokerPartition);
                            addPartitionTopicInfo(currentTopicRegistry, topicDirs, brokerPartition, topic, consumerThreadId);
                            // record the partition ownership decision
                            partitionOwnershipDecision.put(new StringTuple(topic, brokerPartition), consumerThreadId);
                        }
                    }
                }
            }
            //
            /**
             * move the partition ownership here, since that can be used to
             * indicate a truly successful rebalancing attempt A
             * rebalancing attempt is completed successfully only after the
             * fetchers have been started correctly
             */
            if (reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
                logger.info("Updating the cache");
                logger.debug("Partitions per topic cache " + brokerPartitionsPerTopicMap);
                logger.debug("Consumers per topic cache " + consumersPerTopicMap);
                topicRegistry = currentTopicRegistry;
                updateFetcher(cluster, messagesStreams);
                return true;
            } else {
                return false;
            }
            ////////////////////////////
        }

        /**
         * @param cluster
         * @param messagesStreams2
         */
        private void updateFetcher(Cluster cluster, Map<String, List<MessageStream<T>>> messagesStreams2) {
            if (fetcher != null) {
                List<PartitionTopicInfo> allPartitionInfos = new ArrayList<PartitionTopicInfo>();
                for (Pool<Partition, PartitionTopicInfo> p : topicRegistry.values()) {
                    allPartitionInfos.addAll(p.values());
                }
                fetcher.startConnections(allPartitionInfos, cluster, messagesStreams2);
            }
        }

        private boolean reflectPartitionOwnershipDecision(Map<StringTuple, String> partitionOwnershipDecision) {
            final List<StringTuple> successfullyOwnerdPartitions = new ArrayList<StringTuple>();
            int hasPartitionOwnershipFailed = 0;
            for (Map.Entry<StringTuple, String> e : partitionOwnershipDecision.entrySet()) {
                final String topic = e.getKey().k;
                final String brokerPartition = e.getKey().v;
                final String consumerThreadId = e.getValue();
                final ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
                final String partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + brokerPartition;
                try {
                    ZkUtils.createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
                    successfullyOwnerdPartitions.add(new StringTuple(topic, brokerPartition));
                } catch (ZkNodeExistsException e2) {
                    logger.info("waiting for the partition ownership to be deleted: " + brokerPartition);
                    hasPartitionOwnershipFailed++;
                }
            }
            //
            if (hasPartitionOwnershipFailed > 0) {
                for (StringTuple topicAndPartition : successfullyOwnerdPartitions) {
                    deletePartitionOwnershipFromZK(topicAndPartition.k, topicAndPartition.v);
                }
                return false;
            }
            return true;
        }

        /**
         * @param currentTopicRegistry
         * @param topicDirs
         * @param brokerPartition broker-partition format
         * @param topic
         * @param consumerThreadId
         */
        private void addPartitionTopicInfo(Pool<String, Pool<Partition, PartitionTopicInfo>> currentTopicRegistry, ZkGroupTopicDirs topicDirs,
                String brokerPartition, String topic, String consumerThreadId) {
            Partition partition = Partition.parse(brokerPartition);
            Pool<Partition, PartitionTopicInfo> partTopicInfoMap = currentTopicRegistry.get(topic);

            final String znode = topicDirs.consumerOffsetDir + "/" + partition.getName();
            String offsetString = ZkUtils.readDataMaybeNull(zkClient, znode);
            // If first time starting a consumer, set the initial offset based on the config
            long offset = 0L;
            if (offsetString == null) {
                if (OffsetRequest.SMALLES_TTIME_STRING.equals(config.getAutoOffsetReset())) {
                    offset = earliestOrLatestOffset(topic, partition.brokerId, partition.partId, OffsetRequest.EARLIES_TTIME);
                } else if (OffsetRequest.LARGEST_TIME_STRING.equals(config.getAutoOffsetReset())) {
                    offset = earliestOrLatestOffset(topic, partition.brokerId, partition.partId, OffsetRequest.LATES_TTIME);
                } else {
                    throw new InvalidConfigException("Wrong value in autoOffsetReset in ConsumerConfig");
                }

            } else {
                offset = Long.parseLong(offsetString);
            }
            BlockingQueue<FetchedDataChunk> queue = queues.get(new StringTuple(topic, consumerThreadId));
            AtomicLong consumedOffset = new AtomicLong(offset);
            AtomicLong fetchedOffset = new AtomicLong(offset);
            PartitionTopicInfo partTopicInfo = new PartitionTopicInfo(topic,//
                    partition,//
                    queue,//
                    consumedOffset,//
                    fetchedOffset);//
            partTopicInfoMap.put(partition, partTopicInfo);
            logger.debug(partTopicInfo + " selected new offset " + offset);
        }

        private long earliestOrLatestOffset(String topic, int brokerId, int partitionId, long earliestOrLatest) {
            SimpleConsumer simpleConsumer = null;
            long producedOffset = -1;

            try {
                Cluster cluster = ZkUtils.getCluster(zkClient);
                Broker broker = cluster.getBroker(brokerId);
                if (broker == null) {
                    throw new IllegalStateException("Broker " + brokerId + " is unavailable. Cannot issue getOffsetsBefore request");
                }
                //
                //using default value???
                simpleConsumer = new SimpleConsumer(broker.host, broker.port, config.getSocketTimeoutMs(), config.getSocketBufferSize());
                long[] offsets = simpleConsumer.getOffsetsBefore(topic, partitionId, earliestOrLatest, 1);
                //FIXME: what's this!!!
                if (offsets.length > 0) {
                    producedOffset = offsets[0];
                }
            } catch (Exception e) {
                logger.error("error in earliestOrLatestOffset() ", e);
            } finally {
                if (simpleConsumer != null) {
                    Closer.closeQuietly(simpleConsumer);
                }
            }
            return producedOffset;
        }

        private void releasePartitionOwnership(Pool<String, Pool<Partition, PartitionTopicInfo>> localTopicRegistry) {
            logger.info("Releasing partition ownership");
            for (Map.Entry<String, Pool<Partition, PartitionTopicInfo>> e : localTopicRegistry.entrySet()) {
                for (Partition partition : e.getValue().keySet()) {
                    deletePartitionOwnershipFromZK(e.getKey(), partition);
                }
            }
            localTopicRegistry.clear();//clear all
        }

        private void deletePartitionOwnershipFromZK(String topic, String partitionStr) {
            ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
            final String znode = topicDirs.consumerOwnerDir + "/" + partitionStr;
            ZkUtils.deletePath(zkClient, znode);
            logger.debug("Consumer " + consumerIdString + " releasing " + znode);
        }

        private void deletePartitionOwnershipFromZK(String topic, Partition partition) {
            this.deletePartitionOwnershipFromZK(topic, partition.toString());
        }

        /**
         * @param cluster
         * @param messagesStreams2
         * @param myTopicThreadIdsMap
         */
        private void closeFetchers(Cluster cluster, Map<String, List<MessageStream<T>>> messagesStreams2, Map<String, Set<String>> myTopicThreadIdsMap) {
            // topicRegistry.values()
            List<BlockingQueue<FetchedDataChunk>> queuesToBeCleared = new ArrayList<BlockingQueue<FetchedDataChunk>>();
            for (Map.Entry<StringTuple, BlockingQueue<FetchedDataChunk>> e : queues.entrySet()) {
                if (myTopicThreadIdsMap.containsKey(e.getKey().k)) {
                    queuesToBeCleared.add(e.getValue());
                }
            }
            closeFetchersForQueues(cluster, messagesStreams2, queuesToBeCleared);
        }

        private void closeFetchersForQueues(Cluster cluster, Map<String, List<MessageStream<T>>> messageStreams,
                Collection<BlockingQueue<FetchedDataChunk>> queuesToBeCleared) {
            if (fetcher == null) {
                return;
            }
            fetcher.stopConnectionsToAllBrokers();
            fetcher.clearFetcherQueues(queuesToBeCleared, messageStreams.values());
        }

        private void resetState() {
            topicRegistry.clear();
        }

        ////////////////////////////////////////////////////////////
    }

    class ZKSessionExpireListener<T> implements IZkStateListener {

        private final ZkGroupDirs zkGroupDirs;

        private String consumerIdString;

        private TopicCount topicCount;

        private ZKRebalancerListener<T> loadRebalancerListener;

        /**
         * @param zkGroupDirs
         * @param consumerIdString
         * @param topicCount
         * @param loadRebalancerListener
         */
        public ZKSessionExpireListener(ZkGroupDirs zkGroupDirs, String consumerIdString, TopicCount topicCount, ZKRebalancerListener<T> loadRebalancerListener) {
            super();
            this.zkGroupDirs = zkGroupDirs;
            this.consumerIdString = consumerIdString;
            this.topicCount = topicCount;
            this.loadRebalancerListener = loadRebalancerListener;
        }

        public void handleNewSession() throws Exception {
            //Called after the zookeeper session has expired and a new session has been created. You would have to re-create
            // any ephemeral nodes here.
            //
            /**
             * When we get a SessionExpired event, we lost all ephemeral
             * nodes and zkclient has reestablished a connection for us. We
             * need to release the ownership of the current consumer and
             * re-register this consumer in the consumer registry and
             * trigger a rebalance.
             */
            logger.info("Zk expired; release old broker partition ownership; re-register consumer " + consumerIdString);
            loadRebalancerListener.resetState();
            registerConsumerInZK(zkGroupDirs, consumerIdString, topicCount);
            //explicitly trigger load balancing for this consumer
            loadRebalancerListener.syncedRebalance();
            //
            // There is no need to resubscribe to child and state changes.
            // The child change watchers will be set inside rebalance when we read the children list.
        }

        public void handleStateChanged(KeeperState state) throws Exception {
        }
    }

    /** 
     * register consumer data in zookeeper
     * <p>
     * register path: /consumers/groupid/ids/groupid-consumerid
     * <br/>
     * data: {topic:count,topic:count}
     * </p>
     * @param zkGroupDirs zookeeper group path
     * @param consumerIdString groupid-consumerid
     * @param topicCount topic count
     */
    private void registerConsumerInZK(ZkGroupDirs zkGroupDirs, String consumerIdString, TopicCount topicCount) {
        final String path = zkGroupDirs.consumerRegistryDir + "/" + consumerIdString;
        final String data = topicCount.toJsonString();
        logger.info(format("register consumer in zookeeper [%s] => [%s]", path, data));
        ZkUtils.createEphemeralPathExpectConflict(zkClient, path, data);
    }

}
