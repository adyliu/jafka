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

import static com.sohu.jafka.utils.Utils.*;

import java.util.Properties;


/**
 * Configuration for Zookeeper
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZKConfig {

    protected final Properties props;
    
    public ZKConfig(Properties props) {
        this.props = props;
    }

    /** ZK host string */
    public String getZkConnect() {
        return getString(props, "zk.connect", null);
    }

    /** zookeeper session timeout */
    public int getZkSessionTimeoutMs() {
        return getInt(props, "zk.sessiontimeout.ms", 6000);
    }

    /**
     * the max time that the client waits to establish a connection to
     * zookeeper
     */
    public int getZkConnectionTimeoutMs() {
        return getInt(props, "zk.connectiontimeout.ms", 6000);
    }

    /** how far a ZK follower can be behind a ZK leader */
    public int getZkSyncTimeMs() {
        return getInt(props, "zk.synctime.ms", 2000);
    }
    
    protected int get(String name,int defaultValue) {
        return getInt(props,name,defaultValue);
    }
    
    protected String get(String name,String defaultValue) {
        return getString(props,name,defaultValue);
    }
}
