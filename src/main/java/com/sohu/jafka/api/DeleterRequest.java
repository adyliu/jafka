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

import com.sohu.jafka.common.annotations.ClientSide;
import com.sohu.jafka.common.annotations.ServerSide;
import com.sohu.jafka.network.Request;
import com.sohu.jafka.utils.Utils;

/**
 * create a delete request
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
@ClientSide
@ServerSide
public class DeleterRequest implements Request {

    public final String topic;

    public final String password;

    public DeleterRequest(String topic, String password) {
        this.topic = topic;
        this.password = password;
    }

    @Override
    public int getSizeInBytes() {
        return Utils.caculateShortString(topic) + Utils.caculateShortString(password);
    }

    @Override
    public String toString() {
        return "DeleterRequest [topic=" + topic + ", password=" + password + "]";
    }

    @Override
    public RequestKeys getRequestKey() {
        return RequestKeys.DELETE;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic);
        Utils.writeShortString(buffer, password);
    }

    public static DeleterRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        String password = Utils.readShortString(buffer);
        return new DeleterRequest(topic, password);
    }
}
