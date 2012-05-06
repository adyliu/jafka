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
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.network.Request;
import com.sohu.jafka.utils.Utils;

/**
 * message producer request
 * <p>
 * request format:
 * 
 * <pre>
 * topic + partition + messageSize + message
 * =====================================
 *     topic: size(2bytes) + data(utf-8 bytes)
 *     partition: int(4bytes)
 *     messageSize: int(4bytes)
 *     message: bytes
 * </pre>
 * 
 * </p>
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
@ServerSide
public class ProducerRequest implements Request {

    public static final int RandomPartition = -1;

    /**
     * read a producer request from buffer
     * 
     * @param buffer data buffer
     * @return parsed producer request
     */
    public static ProducerRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        int messageSetSize = buffer.getInt();
        ByteBuffer messageSetBuffer = buffer.slice();
        messageSetBuffer.limit(messageSetSize);
        buffer.position(buffer.position() + messageSetSize);
        return new ProducerRequest(topic, partition, new ByteBufferMessageSet(messageSetBuffer));
    }

    /**
     * request messages
     */
    public final ByteBufferMessageSet messages;

    /**
     * topic partition
     */
    public final int partition;

    /**
     * topic name
     */
    public final String topic;

    public ProducerRequest(String topic, int partition, ByteBufferMessageSet messages) {
        this.topic = topic;
        this.partition = partition;
        this.messages = messages;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.PRODUCE;
    }

    public int getSizeInBytes() {
        return (int) (Utils.caculateShortString(topic) + 4 + 4 + messages.getSizeInBytes());
    }

    public int getTranslatedPartition(PartitionChooser chooser) {
        if (partition == RandomPartition) {
            return chooser.choosePartition(topic);
        }
        return partition;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("ProducerRequest(");
        buf.append(topic).append(',').append(partition).append(',');
        buf.append(messages.getSizeInBytes()).append(')');
        return buf.toString();
    }

    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        final ByteBuffer sourceBuffer = messages.serialized();
        buffer.putInt(sourceBuffer.limit());
        buffer.put(sourceBuffer);
        sourceBuffer.rewind();
    }
}
