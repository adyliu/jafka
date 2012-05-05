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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class FetchRequestTest {

    private FetchRequest req = null;

    @Before
    public void createRequest() {
        req = new FetchRequest("demo", 0, 0, 1024 * 1024);
    }

    /**
     * Test method for {@link com.sohu.jafka.api.FetchRequest#getSizeInBytes()}.
     */
    @Test
    public void testGetSizeInBytes() {
        assertEquals(16 + 2+req.topic.length(), req.getSizeInBytes());
    }

    /**
     * Test method for {@link com.sohu.jafka.api.FetchRequest#writeTo(java.nio.ByteBuffer)}.
     */
    @Test
    public void testWriteTo() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        req.writeTo(buffer);
        buffer.flip();
        assertEquals(req.getSizeInBytes(), buffer.limit());
        buffer.clear();
    }

    /**
     * Test method for {@link com.sohu.jafka.api.FetchRequest#readFrom(java.nio.ByteBuffer)}.
     */
    @Test
    public void testReadFrom() {
        final String topic = "demo";
        final int partition = 200;
        final long offset = 1000L;
        final int maxSize = 1024 * 2048;
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxSize);
        buffer.rewind();
        //
        FetchRequest request = FetchRequest.readFrom(buffer);
        assertEquals(topic, request.topic);
        assertEquals(partition, request.partition);
        assertEquals(offset, request.offset);
        assertEquals(maxSize, request.maxSize);
        buffer.clear();
    }

}
