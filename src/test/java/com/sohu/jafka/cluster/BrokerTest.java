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

package com.sohu.jafka.cluster;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class BrokerTest {

    private Broker broker;
    private static final String creatorId = "localhost-" + System.currentTimeMillis();

    @Before
    public void createBroker() {
        broker = new Broker(1, creatorId, "localhost", 9022,true);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.cluster.Broker#Broker(int, java.lang.String, java.lang.String, int)}
     * .
     */
    @Test
    public void testBroker() {
        assertEquals(1, broker.id);
        assertEquals("localhost", broker.host);
        assertEquals(9022, broker.port);
    }

    /**
     * Test method for {@link com.sohu.jafka.cluster.Broker#getZKString()}.
     */
    @Test
    public void testGetZKString() {
        assertEquals(creatorId + ":localhost:9022:true", broker.getZKString());
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.cluster.Broker#createBroker(int, java.lang.String)}.
     */
    @Test
    public void testCreateBroker() {
        String brokerInfoString = broker.getZKString();
        Broker b = Broker.createBroker(broker.id, brokerInfoString);
        assertEquals(broker, b);
        //
        b = new Broker(3, "f700:8000:12d:7306:c0c0:d08a:2315-1366893045642", "f700:8000:12d:7306:c0c0:d08a:2315", 8888,true);
        brokerInfoString = b.getZKString();
        Broker b2 = Broker.createBroker(3, brokerInfoString);
        assertEquals(b, b2);
    }

}
