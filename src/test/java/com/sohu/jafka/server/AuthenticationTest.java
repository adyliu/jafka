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

package com.sohu.jafka.server;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sohu.jafka.server.Authentication.Crc32Auth;
import com.sohu.jafka.server.Authentication.Md5Auth;
import com.sohu.jafka.server.Authentication.PlainAuth;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
public class AuthenticationTest {

    /**
     * Test method for
     * {@link com.sohu.jafka.server.Authentication#auth(java.lang.String)}.
     */
    @Test
    public void testAuth() {
        Authentication auth = Authentication.build("plain:jafka");
        assertTrue(auth.auth("jafka"));
        assertTrue(!auth.auth("hello"));
        //
        auth = Authentication.build("md5: 77be29f6d71ec4e310766ddf881ae6a0");
        assertTrue(auth.auth("jafka"));
        assertTrue(!auth.auth("hello"));
        //
        auth = Authentication.build(null);
        assertTrue(auth.auth("jafka"));
        assertTrue(auth.auth("hello"));
        assertTrue(auth.auth(null));
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.server.Authentication#build(java.lang.String)}
     * .
     */
    @Test
    public void testBuild() {
        Authentication auth = Authentication.build("plain:jafka");
        assertTrue(auth instanceof PlainAuth);
        //
        auth = Authentication.build("crc32     :  1725717671  ");
        assertTrue(auth instanceof Crc32Auth);
        //
        auth = Authentication.build("md5: 77be29f6d71ec4e310766ddf881ae6a0");
        assertTrue(auth instanceof Md5Auth);
        try {
            Authentication.build("jafka");
            fail();
        } catch (IllegalArgumentException e) {}
    }

}
