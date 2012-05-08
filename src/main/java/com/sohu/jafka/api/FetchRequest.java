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
 * @since 1.0
 */
@ClientSide
@ServerSide
public class FetchRequest implements Request {

    /**
     * message topic
     */
    public final String topic;

    /**
     * partition of log file
     */
    public final int partition;

    /**
     * ofset of topic(log file)
     */
    public final long offset;

    /**
     * the max data size in bytes for this request
     */
    public final int maxSize;

    public FetchRequest(String topic, int partition, long offset) {
        this(topic, partition, offset, 64 * 1024);//64KB
    }
    /**
     * create a fetch request
     * 
     * @param topic the topic with messages
     * @param partition the partition of log file
     * @param offset offset of the topic(log file)
     * @param maxSize the max data size in bytes
     */
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
        return RequestKeys.FETCH;
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
