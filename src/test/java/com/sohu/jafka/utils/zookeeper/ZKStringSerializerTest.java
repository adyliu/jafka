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

package com.sohu.jafka.utils.zookeeper;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;


/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZKStringSerializerTest {

    /**
     * Test method for {@link com.sohu.jafka.utils.zookeeper.ZKStringSerializer#serialize(java.lang.Object)}.
     * @throws UnsupportedEncodingException 
     */
    @Test
    public void testSerialize() throws UnsupportedEncodingException {
        byte[] dat = ZKStringSerializer.getInstance().serialize("demo");
        assertArrayEquals("demo".getBytes("UTF-8"), dat);
        assertArrayEquals("中文".getBytes("UTF-8"), ZKStringSerializer.getInstance().serialize("中文"));
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.zookeeper.ZKStringSerializer#deserialize(byte[])}.
     */
    @Test
    public void testDeserialize() {
        final byte[] data = "www.sohu.com".getBytes();
        assertEquals("www.sohu.com", ZKStringSerializer.getInstance().deserialize(data));
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.zookeeper.ZKStringSerializer#getInstance()}.
     */
    @Test
    public void testGetInstance() {
        assertEquals(ZKStringSerializer.getInstance(), ZKStringSerializer.getInstance());
    }

}
