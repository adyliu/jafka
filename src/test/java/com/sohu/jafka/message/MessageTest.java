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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.sohu.jafka.common.UnknownMagicByteException;
import com.sohu.jafka.utils.Utils;


/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class MessageTest {

    /**
     * Test method for {@link com.sohu.jafka.message.Message#crcOffset(byte)}.
     */
    @Test
    public void testCrcOffset() {
        assertEquals(2, Message.crcOffset((byte)1));
        try {
            Message.crcOffset((byte)0);
            fail();
        } catch (UnknownMagicByteException e) {
            //ignore
        }
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#payloadOffset(byte)}.
     */
    @Test
    public void testPayloadOffset() {
        assertEquals(6, Message.payloadOffset((byte)1));
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#headerSize(byte)}.
     */
    @Test
    public void testHeaderSize() {
        assertEquals(6, Message.headerSize((byte)1));
    }

   

    /**
     * Test method for {@link com.sohu.jafka.message.Message#getSizeInBytes()}.
     */
    @Test
    public void testGetSizeInBytes() {
        Message m = new Message("demo".getBytes());
        assertEquals(6+4, m.getSizeInBytes());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#magic()}.
     */
    @Test
    public void testMagic() {
        Message m = new Message("demo".getBytes());
        assertEquals(1,m.magic());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#payloadSize()}.
     */
    @Test
    public void testPayloadSize() {
        Message m = new Message("demo".getBytes());
        assertEquals(4,m.payloadSize());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#attributes()}.
     */
    @Test
    public void testAttributes() {
        Message m = new Message("demo".getBytes());
        assertEquals((byte)0,m.attributes());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#compressionCodec()}.
     */
    @Test
    public void testCompressionCodec() {
        Message m = new Message("demo".getBytes());
        assertEquals(CompressionCodec.NoCompressionCodec, m.compressionCodec());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#checksum()}.
     */
    @Test
    public void testChecksum() {
        Message m = new Message("demo".getBytes());
        assertEquals(Utils.crc32("demo".getBytes()), m.checksum());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#payload()}.
     */
    @Test
    public void testPayload() {
        Message m = new Message("demo".getBytes());
        assertEquals(ByteBuffer.wrap("demo".getBytes()), m.payload());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#isValid()}.
     */
    @Test
    public void testIsValid() {
        Message m = new Message("demo".getBytes());
        ByteBuffer buf = m.buffer;
        assertTrue(m.isValid());
        buf.put(buf.limit()-1, (byte)(1+ buf.get(buf.limit()-1)));
        assertFalse(m.isValid());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#serializedSize()}.
     */
    @Test
    public void testSerializedSize() {
        Message m = new Message("demo".getBytes());
        assertEquals(4+6+("demo".length()),m.serializedSize());
    }

    /**
     * Test method for {@link com.sohu.jafka.message.Message#serializeTo(java.nio.ByteBuffer)}.
     */
    @Test
    public void testSerializeTo() {
        Message m = new Message("demo".getBytes());
        ByteBuffer buffer = ByteBuffer.allocate(m.serializedSize());
        m.serializeTo(buffer);
        assertFalse(buffer.hasRemaining());
    }

}
