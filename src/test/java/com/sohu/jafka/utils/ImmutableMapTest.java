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

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ImmutableMapTest {

    /**
     * Test method for
     * {@link com.sohu.jafka.utils.ImmutableMap#of(java.lang.Object, java.lang.Object)}
     * .
     */
    @Test
    public void testOfKV() {
        Map<String, Integer> map = ImmutableMap.of("demo", 100);
        assertEquals(1, map.size());
        assertTrue(map.containsKey("demo"));
        assertEquals(new Integer(100), map.get("demo"));
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.utils.ImmutableMap#of(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)}
     * .
     */
    @Test
    public void testOfKVKV() {
        assertEquals(2, ImmutableMap.of("demo", 100, "demo2", 2).size());
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.utils.ImmutableMap#of(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)}
     * .
     */
    @Test
    public void testOfKVKVKV() {
        assertEquals(3, ImmutableMap.of("demo", 100, "demo2", 2, "demo3", 300).size());
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.utils.ImmutableMap#of(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)}
     * .
     */
    @Test
    public void testOfKVKVKVKV() {
        assertEquals(4, ImmutableMap.of("demo", 100, "demo2", 2, "demo3", 300, "demo4", 1000).size());
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.utils.ImmutableMap#of(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)}
     * .
     */
    @Test
    public void testOfKVKVKVKVKV() {
        assertEquals(5, ImmutableMap.of("demo", 100, "demo2", 2, "demo3", 300, "demo4", 1000, "demo5", 0).size());
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.utils.ImmutableMap#of(com.sohu.jafka.utils.KV)}
     * .
     */
    @Test
    public void testOfKVOfKV() {
        Map<String, Integer> map = ImmutableMap.of(new KV<String, Integer>("demo", 100));
        assertEquals(1, map.size());
        assertEquals("demo", map.keySet().iterator().next());
        assertEquals(new Integer(100), map.values().iterator().next());
    }

}
