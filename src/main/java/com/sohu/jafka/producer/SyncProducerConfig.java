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

package com.sohu.jafka.producer;

import static com.sohu.jafka.utils.Utils.getInt;
import static com.sohu.jafka.utils.Utils.getString;

import java.util.Properties;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SyncProducerConfig implements SyncProducerConfigShared {

    protected final Properties props;

    final int bufferSize;

    final int connectTimeoutMs;

    final int socketTimeoutMs;

    final int reconnectInterval;

    final int reconnectTimeInterval;

    final int maxMessageSize;

    public SyncProducerConfig(Properties props) {
        this.props = props;
        this.bufferSize = getInt(props, "buffer.size", 100 * 1024);
        this.connectTimeoutMs = getInt(props, "connect.timeout.ms", 5000);
        this.socketTimeoutMs = getInt(props, "socket.timeout.ms", 30000);
        this.reconnectInterval = getInt(props, "reconnect.interval", 100000);
        this.reconnectTimeInterval = getInt(props, "reconnect.time.interval.ms", 1000 * 1000 * 10);
        this.maxMessageSize = getInt(props, "max.message.size", 1000 * 1000);//1MB
        //
    }

    /**
     * @return the host
     */
    public String getHost() {
        return getString(props, "host");
    }

    /**
     * @return the port
     */
    public int getPort() {
        return getInt(props, "port");
    }

    public Properties getProperties() {
        return props;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int getReconnectInterval() {
        return reconnectInterval;
    }

    public int getReconnectTimeInterval() {
        return reconnectTimeInterval;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public String getSerializerClass() {
        return getString(props, "serializer.class", com.sohu.jafka.producer.serializer.DefaultEncoders.class.getName());
    }
}
