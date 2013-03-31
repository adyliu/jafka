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

package com.sohu.jafka.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * Message set helper functions
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public abstract class MessageSet implements Iterable<MessageAndOffset> {

    public static final MessageSet Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0));

    public static final int LogOverhead = 4;

    public static ByteBuffer createByteBuffer(CompressionCodec compressionCodec, Message... messages) {
        if (compressionCodec == CompressionCodec.NoCompressionCodec) {
            ByteBuffer buffer = ByteBuffer.allocate(messageSetSize(messages));
            for (Message message : messages) {
                message.serializeTo(buffer);
            }
            buffer.rewind();
            return buffer;
        }
        //
        if (messages.length == 0) {
            ByteBuffer buffer = ByteBuffer.allocate(messageSetSize(messages));
            buffer.rewind();
            return buffer;
        }
        //
        Message message = CompressionUtils.compress(messages, compressionCodec);
        ByteBuffer buffer = ByteBuffer.allocate(message.serializedSize());
        message.serializeTo(buffer);
        buffer.rewind();
        return buffer;
    }

    public static int entrySize(Message message) {
        return LogOverhead + message.getSizeInBytes();
    }

    public static int messageSetSize(Iterable<Message> messages) {
        int size = 0;
        for (Message message : messages) {
            size += entrySize(message);
        }
        return size;
    }

    public static int messageSetSize(Message... messages) {
        int size = 0;
        for (Message message : messages) {
            size += entrySize(message);
        }
        return size;
    }

    /**
     * get the total size of this message set in bytes
     */
    public abstract long getSizeInBytes();

    /**
     * Validate the checksum of all the messages in the set. Throws an
     * InvalidMessageException if the checksum doesn't match the payload
     * for any message.
     */
    public void validate() {
        for (MessageAndOffset messageAndOffset : this)
            if (!messageAndOffset.message.isValid()) {
                throw new InvalidMessageException();
            }
    }

    /////////////////////////////////////////////////////////////////////////
    //                             abstract method
    /////////////////////////////////////////////////////////////////////////
    /**
     * Write the messages in this set to the given channel starting at the
     * given offset byte. Less than the complete amount may be written, but
     * no more than maxSize can be. The number of bytes written is returned
     * @throws IOException 
     */
    public abstract long writeTo(GatheringByteChannel channel, long offset, long maxSize) throws IOException;
}
