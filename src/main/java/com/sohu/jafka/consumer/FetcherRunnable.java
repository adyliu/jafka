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

import com.github.zkclient.ZkClient;
import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.api.MultiFetchResponse;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.cluster.Broker;
import com.sohu.jafka.cluster.Partition;
import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.common.annotations.ClientSide;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.utils.Closer;
import com.sohu.jafka.utils.zookeeper.ZkGroupTopicDirs;
import com.sohu.jafka.utils.zookeeper.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
public class FetcherRunnable extends Thread {

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final SimpleConsumer simpleConsumer;

    private volatile boolean stopped = false;

    private final ConsumerConfig config;

    private final Broker broker;

    private final ZkClient zkClient;

    private final List<PartitionTopicInfo> partitionTopicInfos;

    private final Logger logger = LoggerFactory.getLogger(FetcherRunnable.class);

    private final static AtomicInteger threadIndex = new AtomicInteger(0);

    public FetcherRunnable(String name,//
                           ZkClient zkClient,//
                           ConsumerConfig config,//
                           Broker broker,//
                           List<PartitionTopicInfo> partitionTopicInfos) {
        super(name + "-" + threadIndex.getAndIncrement());
        this.zkClient = zkClient;
        this.config = config;
        this.broker = broker;
        this.partitionTopicInfos = partitionTopicInfos;
        this.simpleConsumer = new SimpleConsumer(broker.host, broker.port, config.getSocketTimeoutMs(),
                config.getSocketBufferSize());
    }

    public void shutdown() throws InterruptedException {
        logger.info("shutdown the fetcher " + getName());
        stopped = true;
        interrupt();
        shutdownLatch.await(5, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        StringBuilder buf = new StringBuilder("[");
        for (PartitionTopicInfo pti : partitionTopicInfos) {
            buf.append(format("%s-%d-%d,", pti.topic, pti.partition.brokerId, pti.partition.partId));
        }
        buf.append(']');
        logger.info(String.format("%s comsume at %s:%d with %s", getName(), broker.host, broker.port, buf.toString()));
        //
        try {
            final long maxFetchBackoffMs = config.getMaxFetchBackoffMs();
            long fetchBackoffMs = config.getFetchBackoffMs();
            while (!stopped) {
                if (fetchOnce() == 0) {//read empty bytes
                    if (logger.isDebugEnabled()) {
                        logger.debug("backing off " + fetchBackoffMs + " ms");
                    }
                    Thread.sleep(fetchBackoffMs);
                    if (fetchBackoffMs < maxFetchBackoffMs) {
                        fetchBackoffMs += fetchBackoffMs / 10;
                    }
                } else {
                    fetchBackoffMs = config.getFetchBackoffMs();
                }
            }
        } catch (ClosedByInterruptException cbie) {
            logger.info("FetcherRunnable " + this + " interrupted");
        } catch (Exception e) {
            if (stopped) {
                logger.info("FetcherRunnable " + this + " interrupted");
            } else {
                logger.error("error in FetcherRunnable ", e);
            }
        }
        //
        logger.debug("stopping fetcher " + getName() + " to broker " + broker);
        Closer.closeQuietly(simpleConsumer);
        shutdownComplete();
    }

    private long fetchOnce() throws IOException, InterruptedException {
        List<FetchRequest> fetches = new ArrayList<FetchRequest>();
        for (PartitionTopicInfo info : partitionTopicInfos) {
            fetches.add(new FetchRequest(info.topic, info.partition.partId, info.getFetchedOffset(), config
                    .getFetchSize()));
        }
        MultiFetchResponse response = simpleConsumer.multifetch(fetches);
        int index = 0;
        long read = 0L;
        for (ByteBufferMessageSet messages : response) {
            PartitionTopicInfo info = partitionTopicInfos.get(index);
            //
            try {
                read += processMessages(messages, info);
            } catch (IOException e) {
                throw e;
            } catch (InterruptedException e) {
                if (!stopped) {
                    logger.error("error in FetcherRunnable for " + info, e);
                    info.enqueueError(e, info.getFetchedOffset());
                }
                throw e;
            } catch (RuntimeException e) {
                if (!stopped) {
                    logger.error("error in FetcherRunnable for " + info, e);
                    info.enqueueError(e, info.getFetchedOffset());
                }
                throw e;
            }

            //
            index++;
        }
        return read;
    }

    private long processMessages(ByteBufferMessageSet messages, PartitionTopicInfo info) throws IOException,
            InterruptedException {
        boolean done = false;
        if (messages.getErrorCode() == ErrorMapping.OffsetOutOfRangeCode) {
            logger.warn("offset for " + info + " out of range, now we fix it");
            long resetOffset = resetConsumerOffsets(info.topic, info.partition);
            if (resetOffset >= 0) {
                info.resetFetchOffset(resetOffset);
                info.resetConsumeOffset(resetOffset);
                done = true;
            }
        }
        if (!done) {
            return info.enqueue(messages, info.getFetchedOffset());
        }
        return 0;
    }

    private void shutdownComplete() {
        this.shutdownLatch.countDown();
    }

    private long resetConsumerOffsets(String topic, Partition partition) throws IOException {
        long offset = -1;
        String autoOffsetReset = config.getAutoOffsetReset();
        if (OffsetRequest.SMALLES_TIME_STRING.equals(autoOffsetReset)) {
            offset = OffsetRequest.EARLIES_TTIME;
        } else if (OffsetRequest.LARGEST_TIME_STRING.equals(autoOffsetReset)) {
            offset = OffsetRequest.LATES_TTIME;
        }
        //
        final ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(config.getGroupId(), topic);
        long[] offsets = simpleConsumer.getOffsetsBefore(topic, partition.partId, offset, 1);
        //TODO: fixed no offsets?
        ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + partition.getName(), "" + offsets[0]);
        return offsets[0];
    }
}
