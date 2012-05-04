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

import org.junit.Test;


/**
 * @author adyliu (imxylz@gmail.com)
 * @since <version>
 */
public class KVTest {

    /**
     * Test method for {@link com.sohu.jafka.utils.KV#KV(java.lang.Object, java.lang.Object)}.
     */
    @Test
    public void testKV() {
        KV<String,Long> kv = new KV<String, Long>("demo",100L);
        assertEquals("demo", kv.k);
        assertEquals(100L, kv.v.longValue());
    }

    /**
     * Test method for {@link com.sohu.jafka.utils.KV#equals(java.lang.Object)}.
     */
    @Test
    public void testEqualsObject() {
       assertEquals(new KV<String,Long>("demo",100L),new KV<String,Long>("demo",100L));
    }

}
