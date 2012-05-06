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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.junit.Test;

import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ProducerRequestTest {

    /**
     * Test method for {@link com.sohu.jafka.api.ProducerRequest#readFrom(java.nio.ByteBuffer)}
     * .
     */
    @Test
    public void testReadFrom() {
        final int partition = 100;
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Utils.writeShortString(buffer, "demo");

        ByteBufferMessageSet ms = createMessageSet("message1","message2");
        //
        buffer.putInt(partition);//paritition
        buffer.putInt((int) ms.getSizeInBytes());
        //
        ms.getBuffer().mark();
        buffer.put(ms.getBuffer());
        ms.getBuffer().reset();
        //==============write over==============
        buffer.rewind();
        ProducerRequest request = ProducerRequest.readFrom(buffer);
        assertEquals("demo", request.topic);
        assertEquals(partition, request.partition);
        Iterator<MessageAndOffset> messages = request.messages.iterator();
        assertTrue(messages.hasNext());
        assertEquals("message1", Utils.toString(messages.next().message.payload(), "UTF-8"));
        assertTrue(messages.hasNext());
        assertEquals("message2", Utils.toString(messages.next().message.payload(), "UTF-8"));
        assertFalse(messages.hasNext());
    }

    private ByteBufferMessageSet createMessageSet(String...data) {
        Message[] messages = new Message[data.length];
        for(int i=0;i<data.length;i++) {
            messages[i] = new Message(Utils.getBytes(data[i]));
        }
        return new ByteBufferMessageSet(messages);
    }

    /**
     * Test method for {@link com.sohu.jafka.api.ProducerRequest#getSizeInBytes()}.
     */
    @Test
    public void testGetSizeInBytes() {
        ByteBufferMessageSet ms = createMessageSet("m1","m2");
        ProducerRequest request = new ProducerRequest("demo", 2, ms);
        assertEquals(2+4+4+4+ms.getSizeInBytes(), request.getSizeInBytes());
    }


    /**
     * Test method for {@link com.sohu.jafka.api.ProducerRequest#writeTo(java.nio.ByteBuffer)}.
     */
    @Test
    public void testWriteTo() {
        ByteBufferMessageSet ms = createMessageSet("m1","m2");
        ProducerRequest request = new ProducerRequest("demo", 2, ms);
        //
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        request.writeTo(buffer);
        buffer.rewind();
        //read again
        ProducerRequest readRequest = ProducerRequest.readFrom(buffer);
        assertEquals(request.topic, readRequest.topic);
        assertEquals(request.partition, readRequest.partition);
        assertEquals(request.getSizeInBytes(), readRequest.getSizeInBytes());
    }

}
