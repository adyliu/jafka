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

import java.util.Properties;

import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.utils.Utils;
import com.sohu.jafka.utils.ZKConfig;

/**
 * the consumer configuration
 * <p>
 * The minimal configurations have these names:
 * <ul>
 * <li>groupid: the consumer group name</li>
 * <li>zk.connect: the zookeeper connection string</li>
 * </ul>
 * </p>
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ConsumerConfig extends ZKConfig {

    private String groupId;

    private String consumerId;

    private int socketTimeoutMs;

    private int socketBufferSize;

    private int fetchSize;

    private long fetchBackoffMs;

    private long maxFetchBackoffMs;

    private boolean autoCommit;

    private int autoCommitIntervalMs;

    private int maxQueuedChunks;

    private int maxRebalanceRetries;

    private int rebalanceBackoffMs;

    private String autoOffsetReset;

    private int consumerTimeoutMs;

    private String mirrorTopicsWhitelist;

    private String mirrorTopicsBlackList;

    private int mirrorConsumerNumThreads;

    /**
     * <p>
     * The minimal configurations have these names:
     * <ul>
     * <li>groupid: the consumer group name</li>
     * <li>zk.connect: the zookeeper connection string</li>
     * </ul>
     * </p>
     * 
     * @param props config properties
     */
    public ConsumerConfig(Properties props) {
        super(props);
        this.groupId = Utils.getString(props, "groupid");
        this.consumerId = Utils.getString(props, "consumerid", null);
        this.socketTimeoutMs = get("socket.timeout.ms", 30 * 1000);
        this.socketBufferSize = get("socket.buffersize", 64 * 1024);//64KB
        this.fetchSize = get("fetch.size", 1024 * 1024);//1MB
        this.fetchBackoffMs = get("fetcher.backoff.ms", 1000);
        this.maxFetchBackoffMs = get("fetcher.backoff.ms.max", (int) fetchBackoffMs * 10);
        this.autoCommit = Utils.getBoolean(props, "autocommit.enable", true);
        this.autoCommitIntervalMs = get("autocommit.interval.ms", 1000);//1 seconds
        this.maxQueuedChunks = get("queuedchunks.max", 10);
        this.maxRebalanceRetries = get("rebalance.retries.max", 4);
        this.rebalanceBackoffMs = get("rebalance.backoff.ms", 10000);//change default rebalance backoff time to 10 seconds
        this.autoOffsetReset = get("autooffset.reset", OffsetRequest.SMALLES_TIME_STRING);
        this.consumerTimeoutMs = get("consumer.timeout.ms", -1);
        this.mirrorTopicsWhitelist = get("mirror.topics.whitelist", "");
        this.mirrorTopicsBlackList = get("mirror.topics.blacklist", "");
        this.mirrorConsumerNumThreads = get("mirror.consumer.numthreads", 1);

    }

    /**
     * a string that uniquely identifies a set of consumers within the same consumer group
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * consumer id: generated automatically if not set. Set this explicitly for only testing
     * purpose.
     */
    public String getConsumerId() {
        return consumerId;
    }

    /** the socket timeout for network requests */
    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    /** the socket receive buffer for network requests */
    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    /** the number of byes of messages to attempt to fetch */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * to avoid repeatedly polling a broker node which has no new data we will backoff every
     * time we get an empty set from the broker
     */
    public long getFetchBackoffMs() {
        return fetchBackoffMs;
    }

    /**
     * if true, periodically commit to zookeeper the offset of messages already fetched by the
     * consumer
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * the frequency in ms that the consumer offsets are committed to zookeeper
     */
    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    /** max number of messages buffered for consumption */
    public int getMaxQueuedChunks() {
        return maxQueuedChunks;
    }

    /** max number of retries during rebalance */
    public int getMaxRebalanceRetries() {
        return maxRebalanceRetries;
    }

    /** backoff time between retries during rebalance */
    public int getRebalanceBackoffMs() {
        return rebalanceBackoffMs;
    }

    /**
     * what to do if an offset is out of range.
     * 
     * <pre>
     *     smallest : automatically reset the offset to the smallest offset
     *     largest : automatically reset the offset to the largest offset
     *     anything else: throw exception to the consumer
     * </pre>
     */
    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    /**
     * throw a timeout exception to the consumer if no message is available for consumption
     * after the specified interval
     */
    public int getConsumerTimeoutMs() {
        return consumerTimeoutMs;
    }

    /**
     * Whitelist of topics for this mirror's embedded consumer to consume. At most one of
     * whitelist/blacklist may be specified.
     */
    public String getMirrorTopicsWhitelist() {
        return mirrorTopicsWhitelist;
    }

    /**
     * Topics to skip mirroring. At most one of whitelist/blacklist may be specified
     */
    public String getMirrorTopicsBlackList() {
        return mirrorTopicsBlackList;
    }

    public int getMirrorConsumerNumThreads() {
        return mirrorConsumerNumThreads;
    }

    public long getMaxFetchBackoffMs() {
        return maxFetchBackoffMs;
    }
}
