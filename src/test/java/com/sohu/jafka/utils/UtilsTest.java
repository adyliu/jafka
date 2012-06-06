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

package com.sohu.jafka.utils;

import static com.sohu.jafka.utils.Utils.caculateShortString;
import static com.sohu.jafka.utils.Utils.crc32;
import static com.sohu.jafka.utils.Utils.getBoolean;
import static com.sohu.jafka.utils.Utils.getInt;
import static com.sohu.jafka.utils.Utils.getProps;
import static com.sohu.jafka.utils.Utils.getString;
import static com.sohu.jafka.utils.Utils.getTopicPartition;
import static com.sohu.jafka.utils.Utils.getTopicRentionHours;
import static com.sohu.jafka.utils.Utils.loadProps;
import static com.sohu.jafka.utils.Utils.readShortString;
import static com.sohu.jafka.utils.Utils.writeShortString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;


/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class UtilsTest {

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#loadProps(java.lang.String)}.
     */
    @Test
    public void testLoadProps() {
        final String filename = "./conf/server.properties";
        if(new File(filename).exists()) {
         Properties props =   loadProps(filename);
         assertTrue(props.size()>0);
        }
        if(! new File(filename+"-1").exists()){
            try {
                loadProps(filename+"-1");
                fail();
            } catch (RuntimeException e) {
                //ignore
            }
        }
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#getProps(java.util.Properties, java.lang.String, java.util.Properties)}.
     */
    @Test
    public void testGetProps() {
        Properties props = new Properties();
        props.setProperty("black.list", "demo=10,test=100");
        //
        Properties vp = getProps(props, "black.list", null);
        assertEquals("10", vp.getProperty("demo"));
        assertEquals("100", vp.getProperty("test"));
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#getString(java.util.Properties, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testGetStringPropertiesStringString() {
        System.setProperty("demo.count", "100");
        assertEquals("-1",getString(System.getProperties(), "demo.count.noexist","-1"));
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#getString(java.util.Properties, java.lang.String)}.
     */
    @Test
    public void testGetStringPropertiesString() {
        System.setProperty("demo.count", "100");
        assertEquals("100",getString(System.getProperties(), "demo.count"));
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#getInt(java.util.Properties, java.lang.String)}.
     */
    @Test
    public void testGetIntPropertiesString() {
        System.setProperty("demo.count", "100");
        assertEquals(100,getInt(System.getProperties(), "demo.count"));
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#getInt(java.util.Properties, java.lang.String, int)}.
     */
    @Test
    public void testGetIntPropertiesStringInt() {
        System.setProperty("demo.count", "100");
        assertEquals(100,getInt(System.getProperties(), "demo.count",200));
        assertEquals(200,getInt(System.getProperties(), "demo.count.noexist",200));
    }


    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#getBoolean(java.util.Properties, java.lang.String, boolean)}.
     */
    @Test
    public void testGetBoolean() {
        System.setProperty("demo.boolean", "true");
        assertTrue(getBoolean(System.getProperties(), "demo.boolean", false));
        assertTrue(getBoolean(System.getProperties(), "demo.boolean.notexist", true));
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#getTopicRentionHours(java.lang.String)}.
     */
    @Test
    public void testGetTopicRentionHours() {
        Map<String,Integer> map = getTopicRentionHours("demo:10,test:20");
        assertEquals(2, map.size());
        assertEquals(new Integer(10),map.get("demo"));
        assertEquals(new Integer(20),map.get("test"));
    }


    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#getTopicPartition(java.lang.String)}.
     */
    @Test
    public void testGetTopicPartition() {
        KV<String, Integer> topicPartition = getTopicPartition("100-2");
        assertEquals("100",topicPartition.k);
        assertEquals(new Integer(2), topicPartition.v);
    }

    
    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#writeShortString(java.nio.ByteBuffer, java.lang.String)}.
     */
    @Test
    public void testWriteShortString() {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        String s = "demo";
        writeShortString(buffer, s);
        assertEquals(2+s.length(), buffer.position());
        buffer.flip();
        assertEquals(2+s.length(), buffer.limit());
        assertEquals(s.length(), buffer.getShort());
    }

   

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#crc32(byte[], int, int)}.
     */
    @Test
    public void testCrc32ByteArrayIntInt() {
        long c1 = crc32("jafka".getBytes());
        long c2 = crc32("jafka".getBytes());
        assertEquals(c1, c2);
        assertTrue(c1>0);
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#readShortString(java.nio.ByteBuffer)}.
     */
    @Test
    public void testReadShortString() throws Exception{
        final String s = "中文";
        final int len = s.getBytes("UTF-8").length;
        final ByteBuffer buffer = ByteBuffer.allocate(2+len);
        buffer.putShort((short)len);
        buffer.put(s.getBytes("UTF-8"));
        buffer.rewind();
        //
        assertEquals(s, readShortString(buffer));
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.Utils#caculateShortString(java.lang.String)}.
     */
    @Test
    public void testCaculateShortString() {
        assertEquals(2+4, caculateShortString("demo"));
        assertEquals(2+6, caculateShortString("中文"));
    }
}
