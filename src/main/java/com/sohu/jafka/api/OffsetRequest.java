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
 * offset request
 * <p>
 * Jafka will returns all offsets earlier than given time with max number
 * limit. The fist offset of result is the biggest and the the last is the
 * smallest.
 * </p>
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
@ServerSide
public class OffsetRequest implements Request {

    public static final String SMALLES_TIME_STRING = "smallest";

    public static final String LARGEST_TIME_STRING = "largest";

    /**
     * reading the latest offset
     */
    public static final long LATES_TTIME = -1L;

    /**
     * reading the earilest offset
     */
    public static final long EARLIES_TTIME = -2L;

    ///////////////////////////////////////////////////////////////////////
    /**
     * message topic
     */
    public String topic;

    /**
     * topic partition,default value is 0
     */
    public int partition;

    /**
     * the earliest time of messages(unix milliseconds time)
     * <p>
     * <ul>
     * <li>{@link #LATES_TTIME}: the latest(largest) offset</li>
     * <li>{@link #EARLIES_TTIME}: the earilest(smallest) offset</li>
     * <li>time&gt;0: the log file offset which lastmodified time earlier
     * than the time</li>
     * </ul>
     * </p>
     */
    public long time;

    /**
     * number of offsets
     */
    public int maxNumOffsets;

    /**
     * create a offset request
     * 
     * @param topic topic name
     * @param partition partition id
     * @param time the log file created time {@link #time}
     * @param maxNumOffsets the number of offsets
     * @see #time
     */
    public OffsetRequest(String topic, int partition, long time, int maxNumOffsets) {
        this.topic = topic;
        this.partition = partition;
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.OFFSETS;
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
