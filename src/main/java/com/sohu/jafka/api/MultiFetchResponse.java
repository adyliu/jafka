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


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.message.ByteBufferMessageSet;

/**
 * a response with mulit-data
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
public class MultiFetchResponse implements Iterable<ByteBufferMessageSet> {

    private final Collection<ByteBufferMessageSet> messageSets;

    public MultiFetchResponse(ByteBuffer buffer, int numSets, List<Long> offsets) {
        super();
        this.messageSets = new ArrayList<ByteBufferMessageSet>();
        for (int i = 0; i < numSets; i++) {
            int size = buffer.getInt();
            short errorCode = buffer.getShort();
            ByteBuffer copy = buffer.slice();
            int payloadSize = size - 2;
            copy.limit(payloadSize);
            buffer.position(buffer.position() + payloadSize);
            messageSets.add(new ByteBufferMessageSet(copy, offsets.get(i), ErrorMapping.valueOf(errorCode)));
        }
    }

    public Iterator<ByteBufferMessageSet> iterator() {
        return messageSets.iterator();
    }

}
