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

package com.sohu.jafka.network;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.message.MessageSet;

/**
 * A zero-copy message response that writes the bytes needed directly from
 * the file wholly in kernel space
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class MessageSetSend extends AbstractSend {

    private long sent = 0;

    private long size;

    private final ByteBuffer header = ByteBuffer.allocate(6);

    //
    public final MessageSet messages;

    public final ErrorMapping errorCode;

    public MessageSetSend(MessageSet messages, ErrorMapping errorCode) {
        super();
        this.messages = messages;
        this.errorCode = errorCode;
        this.size = messages.getSizeInBytes();
        header.putInt((int) (size + 2));
        header.putShort(errorCode.code);
        header.rewind();
    }

    public MessageSetSend(MessageSet messages) {
        this(messages, ErrorMapping.NoError);
    }

    public MessageSetSend() {
        this(MessageSet.Empty);
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        if (header.hasRemaining()) {
            written += channel.write(header);
        }
        if (!header.hasRemaining()) {
            int fileBytesSent = (int) messages.writeTo(channel, sent, size - sent);
            written += fileBytesSent;
            sent += fileBytesSent;
        }
        if (sent >= size) {
            setCompleted();
        }
        return written;
    }

    public int getSendSize() {
        return (int) size + header.capacity();
    }

}
