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

import static java.lang.String.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import com.sohu.jafka.common.annotations.ServerSide;
import com.sohu.jafka.utils.Utils;

/**
 * Receive data from socket
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ServerSide
public class BoundedByteBufferReceive extends AbstractTransmission implements Receive {

    final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

    private ByteBuffer contentBuffer = null;

    private int maxRequestSize;

    public BoundedByteBufferReceive() {
        this(Integer.MAX_VALUE);
    }

    public BoundedByteBufferReceive(int maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
    }

    public int readFrom(ReadableByteChannel channel) throws IOException {
        expectIncomplete();
        int read = 0;
        if (sizeBuffer.remaining() > 0) {
            read += Utils.read(channel, sizeBuffer);
        }
        //
        if (contentBuffer == null && !sizeBuffer.hasRemaining()) {
            sizeBuffer.rewind();
            int size = sizeBuffer.getInt();
            if (size <= 0) {
                throw new InvalidRequestException(format("%d is not a valid request size.", size));
            }
            if (size > maxRequestSize) {
                final String msg = "Request of length %d is not valid, it is larger than the maximum size of %d bytes.";
                throw new InvalidRequestException(format(msg, size, maxRequestSize));
            }
            contentBuffer = byteBufferAllocate(size);
        }
        //
        if (contentBuffer != null) {
            read = Utils.read(channel, contentBuffer);
            //
            if (!contentBuffer.hasRemaining()) {
                contentBuffer.rewind();
                setCompleted();
            }
        }
        return read;
    }

    public int readCompletely(ReadableByteChannel channel) throws IOException {
        int read = 0;
        while (!complete()) {
            read += readFrom(channel);
        }
        return read;
    }

    public ByteBuffer buffer() {
        expectComplete();
        return contentBuffer;
    }

    private ByteBuffer byteBufferAllocate(int size) {
        ByteBuffer buffer = null;
        try {
            buffer = ByteBuffer.allocate(size);
        } catch (OutOfMemoryError oome) {
            throw new RuntimeException("OOME with size " + size, oome);
        } catch (RuntimeException t) {
            throw t;
        }
        return buffer;
    }

    @Override
    public String toString() {
        String msg = "Receive [maxRequestSize=%d, expectSize=%d, readSize=%d, done=%s]";
        return format(msg, maxRequestSize, contentBuffer == null ? -1 : contentBuffer.limit(), //
                contentBuffer == null ? -1 : contentBuffer.position(), //
                complete());
    }
}
