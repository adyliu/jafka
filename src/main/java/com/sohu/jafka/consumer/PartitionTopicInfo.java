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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.sohu.jafka.cluster.Partition;
import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.message.ByteBufferMessageSet;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class PartitionTopicInfo {

    private static final Logger logger = Logger.getLogger(PartitionTopicInfo.class);

    public final String topic;

    public final int brokerId;

    private final BlockingQueue<FetchedDataChunk> chunkQueue;

    private final AtomicLong consumedOffset;

    private final AtomicLong fetchedOffset;

    private final AtomicLong consumedOffsetChanged = new AtomicLong(0);

    final Partition partition;

    public PartitionTopicInfo(String topic, //
            int brokerId, //
            Partition partition,//
            BlockingQueue<FetchedDataChunk> chunkQueue, //
            AtomicLong consumedOffset, //
            AtomicLong fetchedOffset) {
        super();
        this.topic = topic;
        this.partition = partition;
        this.brokerId = brokerId;
        this.chunkQueue = chunkQueue;
        this.consumedOffset = consumedOffset;
        this.fetchedOffset = fetchedOffset;
    }

    public long getConsumedOffset() {
        return consumedOffset.get();
    }

    public AtomicLong getConsumedOffsetChanged() {
        return consumedOffsetChanged;
    }

    public boolean resetComsumedOffsetChanged(long lastChanged) {
        return consumedOffsetChanged.compareAndSet(lastChanged, 0);
    }

    public long getFetchedOffset() {
        return fetchedOffset.get();
    }

    public void resetConsumeOffset(long newConsumeOffset) {
        consumedOffset.set(newConsumeOffset);
        consumedOffsetChanged.incrementAndGet();
    }

    public void resetFetchOffset(long newFetchOffset) {
        fetchedOffset.set(newFetchOffset);
    }

    public long enqueue(ByteBufferMessageSet messages, long fetchOffset) throws InterruptedException {
        long size = messages.getValidBytes();
        if (size > 0) {
            final long oldOffset = fetchedOffset.get();
            chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset));
            long newOffset = fetchedOffset.addAndGet(size);
            if (logger.isDebugEnabled()) {
                logger.debug(format("updated fetchset (origin+size=newOffset) => %d + %d = %d", oldOffset, size, newOffset));
            }
        }
        return size;
    }

    @Override
    public String toString() {
        return topic + ":" + partition + ": fetched offset = " + fetchedOffset.get() + ": consumed offset = " + consumedOffset.get();
    }

    public void enqueueError(Exception e, long fetchOffset) throws InterruptedException {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(ErrorMapping.EMPTY_BUFFER, 0, ErrorMapping.valueOf(e));
        chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset));
    }
}
