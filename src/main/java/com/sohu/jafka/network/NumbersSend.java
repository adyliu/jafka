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
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.1
 */
public abstract class NumbersSend extends AbstractSend {

    protected ByteBuffer header = ByteBuffer.allocate(6);

    protected ByteBuffer contentBuffer;

    NumbersSend() {
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        if (header.hasRemaining()) {
            written += channel.write(header);
        }
        if (!header.hasRemaining() && contentBuffer.hasRemaining()) {
            written += channel.write(contentBuffer);
        }
        if (!contentBuffer.hasRemaining()) {
            setCompleted();
        }
        return written;
    }

    public static class IntegersSend extends NumbersSend {

        public IntegersSend(int... numbers) {
            header.putInt(4 + numbers.length * 4 + 2);
            header.putShort(ErrorMapping.NoError.code);
            header.rewind();
            contentBuffer = Utils.serializeArray(numbers);
        }
    }

    public static class LongsSend extends NumbersSend {

        public LongsSend(long... numbers) {
            header.putInt(4 + numbers.length * 8 + 2);
            header.putShort(ErrorMapping.NoError.code);
            header.rewind();
            contentBuffer = Utils.serializeArray(numbers);
        }
    }
}
