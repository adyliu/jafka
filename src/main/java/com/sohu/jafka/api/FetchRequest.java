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

import com.sohu.jafka.common.annotations.ClientSide;
import com.sohu.jafka.common.annotations.ServerSide;
import com.sohu.jafka.network.Request;
import com.sohu.jafka.utils.Utils;

/**
 * fetching data from server
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
@ClientSide
@ServerSide
public class FetchRequest implements Request {

    private final String topic;

    private final int partition;

    private final long offset;

    private final int maxSize;

    public FetchRequest(String topic, int partition, long offset, int maxSize) {
        this.topic = topic;
        if (topic == null) {
            throw new IllegalArgumentException("no topic");
        }
        this.partition = partition;
        this.offset = offset;
        this.maxSize = maxSize;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.Fetch;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getSizeInBytes() {
        return Utils.caculateShortString(topic) + 4 + 8 + 4;
    }

    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxSize);
    }

    @Override
    public String toString() {
        return "FetchRequest(topic:" + topic + ", part:" + partition + " offset:" + offset + " maxSize:" + maxSize + ")";
    }

    /**
     * Read a fetch request from buffer(socket data)
     * 
     * @param buffer the buffer data
     * @return a fetch request
     * @throws IllegalArgumentException while error data format(no topic)
     */
    public static FetchRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int size = buffer.getInt();
        return new FetchRequest(topic, partition, offset, size);
    }
}
