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

package com.sohu.jafka.common;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ErrorMappingTest {

    /**
     * Test method for {@link com.sohu.jafka.common.ErrorMapping#valueOf(java.lang.Exception)}.
     */
    @Test
    public void testValueOfException() {
        assertEquals(ErrorMapping.OffsetOutOfRangeCode, ErrorMapping.valueOf(new OffsetOutOfRangeException()));
        assertEquals(ErrorMapping.InvalidFetchSizeCode, ErrorMapping.valueOf(new InvalidMessageSizeException()));
        assertEquals(ErrorMapping.UnkonwCode, ErrorMapping.valueOf(new NullPointerException()));
    }

    /**
     * Test method for {@link com.sohu.jafka.common.ErrorMapping#valueOf(short)}.
     */
    @Test
    public void testValueOfShort() {
        assertEquals(ErrorMapping.UnkonwCode, ErrorMapping.valueOf((short) 100));
        for (ErrorMapping em : ErrorMapping.values()) {
            assertEquals(em, ErrorMapping.valueOf(em.code));
        }
    }
}
