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

package com.sohu.jafka.api;

import java.nio.ByteBuffer;
import java.util.List;

import com.sohu.jafka.common.annotations.ClientSide;
import com.sohu.jafka.common.annotations.ServerSide;
import com.sohu.jafka.network.Request;
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
@ClientSide
@ServerSide
public class OffsetRequest implements Request {

    public static final String SmallestTimeString = "smallest";

    public static final String LargestTimeString = "largest";

    public static final long LatestTime = -1L;

    public static final long EarliestTime = -2L;

    ///////////////////////////////////////////////////////////////////////
    private String topic;

    private int partition;

    private long time;

    private int maxNumOffsets;

    public OffsetRequest(String topic, int partition, long time, int maxNumOffsets) {
        this.topic = topic;
        this.partition = partition;
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.Offsets;
    }

    /**
     * @return the maxNumOffsets
     */
    public int getMaxNumOffsets() {
        return maxNumOffsets;
    }

    /**
     * @return the time
     */
    public long getTime() {
        return time;
    }

    /**
     * @return the topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @return the partition
     */
    public int getPartition() {
        return partition;
    }

    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(time);
        buffer.putInt(maxNumOffsets);
    }

    public int getSizeInBytes() {
        return Utils.caculateShortString(topic) + 4 + 8 + 4;
    }

    @Override
    public String toString() {
        return "OffsetRequest(topic:" + topic + ", part:" + partition + ", time:" + time + ", maxNumOffsets:" + maxNumOffsets + ")";
    }

    ///////////////////////////////////////////////////////////////////////
    public static OffsetRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxNumOffsets = buffer.getInt();
        return new OffsetRequest(topic, partition, offset, maxNumOffsets);
    }

    public static ByteBuffer serializeOffsetArray(List<Long> offsets) {
        int size = 4 + 8 * offsets.size();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(offsets.size());
        for (int i = 0; i < offsets.size(); i++) {
            buffer.putLong(offsets.get(i));
        }
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer serializeOffsetArray(long[] offsets) {
        int size = 4 + 8 * offsets.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(offsets.length);
        for (int i = 0; i < offsets.length; i++) {
            buffer.putLong(offsets[i]);
        }
        buffer.rewind();
        return buffer;
    }

    public static long[] deserializeOffsetArray(ByteBuffer buffer) {
        int size = buffer.getInt();
        long[] offsets = new long[size];
        for (int i = 0; i < size; i++) {
            offsets[i] = buffer.getLong();
        }
        return offsets;
    }
}
