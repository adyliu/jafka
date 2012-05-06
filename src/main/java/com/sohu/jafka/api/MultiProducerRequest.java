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
import java.util.List;

import com.sohu.jafka.network.Request;

/**
 * Multi-Messages (maybe without same topic)
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class MultiProducerRequest implements Request {

    public final List<ProducerRequest> produces;

    public MultiProducerRequest(List<ProducerRequest> produces) {
        this.produces = produces;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.MULTIPRODUCE;
    }

    public void writeTo(ByteBuffer buffer) {
        if (produces.size() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Number of requests in MultiFetchRequest exceeds " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) produces.size());
        for (ProducerRequest produce : produces) {
            produce.writeTo(buffer);
        }
    }

    public int getSizeInBytes() {
        int size = 2;
        for (ProducerRequest produce : produces) {
            size += produce.getSizeInBytes();
        }
        return size;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (ProducerRequest produce : produces) {
            buf.append(produce.toString()).append(",");
        }
        return buf.toString();
    }

    public static MultiProducerRequest readFrom(ByteBuffer buffer) {
        int count = buffer.getShort();
        List<ProducerRequest> produces = new ArrayList<ProducerRequest>(count);
        for (int i = 0; i < count; i++) {
            produces.add(ProducerRequest.readFrom(buffer));
        }
        return new MultiProducerRequest(produces);
    }

}
