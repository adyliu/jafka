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

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class BoundedByteBufferSend extends AbstractSend {

    final ByteBuffer buffer;

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

    public BoundedByteBufferSend(ByteBuffer buffer) {
        this.buffer = buffer;
        sizeBuffer.putInt(buffer.limit());
        sizeBuffer.rewind();
    }

    public BoundedByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    public BoundedByteBufferSend(Request request) {
        this(request.getSizeInBytes() + 2);
        buffer.putShort((short)request.getRequestKey().value);
        request.writeTo(buffer);
        buffer.rewind();
    }
    
   
    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        // try to write the size if we haven't already
        if (sizeBuffer.hasRemaining()) written += channel.write(sizeBuffer);
        // try to write the actual buffer itself
        if (!sizeBuffer.hasRemaining() && buffer.hasRemaining()) written += channel.write(buffer);
        // if we are done, mark it off
        if (!buffer.hasRemaining()) { setCompleted();}

        return written;
    }

}
