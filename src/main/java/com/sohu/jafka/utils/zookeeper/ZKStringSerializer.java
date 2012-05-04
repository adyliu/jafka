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

import com.github.zkclient.exception.ZkMarshallingError;
import com.github.zkclient.serialize.ZkSerializer;
import com.sohu.jafka.common.annotations.ThreadSafe;
import com.sohu.jafka.utils.Utils;

/**
 * String encoding for zookeeper data
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ThreadSafe
public class ZKStringSerializer implements ZkSerializer {

    private static final ZKStringSerializer instance = new ZKStringSerializer();

    private ZKStringSerializer() {
    }

    public byte[] serialize(Object data) throws ZkMarshallingError {
        if (data == null) {
            throw new NullPointerException();
        }
        return Utils.getBytes((String) data);
    }

    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return bytes == null ? null : Utils.fromBytes(bytes);
    }

    /**
     * @return the instance
     */
    public static ZKStringSerializer getInstance() {
        return instance;
    }

}
