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

import com.sohu.jafka.log.FixedSizeRollingStrategy;
import com.sohu.jafka.log.RollingStrategy;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.utils.Utils;
import com.sohu.jafka.utils.ZKConfig;

import java.util.Map;
import java.util.Properties;

import static com.sohu.jafka.utils.Utils.*;

/**
 * Configuration for the jafka server
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ServerConfig extends ZKConfig {

    private final Authentication authentication;

    public ServerConfig(Properties props) {
        super(props);
        authentication = Authentication.build(getString(props, "password", null));
    }

    /**
     * the port to listen and accept connections on (default 9092)
     */
    public int getPort() {
        return getInt(props, "port", 9092);
    }
    public int getHttpPort(){ return getInt(props,"http.port",0);}

    /**
     * hostname of broker. If not set, will pick up from the value returned
     * from getLocalHost. If there are multiple interfaces getLocalHost may
     * not be what you want.
     */
    public String getHostName() {
        return getString(props, "hostname", null);
    }

    /**
     * topic.autocreated create new topic after booted
     * @return auto create new topic after booted, default true
     */
    public boolean isTopicAutoCreated(){
        return getBoolean(props,"topic.autocreated",true);
    }

    /**
     * the broker id for this server
     */
    public int getBrokerId() {
        return getIntInRange(props, "brokerid", -1, 0, Integer.MAX_VALUE);
    }

    /**
     * max connection for one jvm (default 10000)
     */
    public int getMaxConnections() {
        return getInt(props, "max.connections", 10000);
    }

    /**
     * the SO_SNDBUFF buffer of the socket sever sockets
     */
    public int getSocketSendBuffer() {
        return getInt(props, "socket.send.buffer", 100 * 1024);
    }

    /**
     * the SO_RCVBUFF buffer of the socket sever sockets
     */
    public int getSocketReceiveBuffer() {
        return getInt(props, "socket.receive.buffer", 100 * 1024);
    }

    /**
     * the maximum number of bytes in a socket request
     */
    public int getMaxSocketRequestSize() {
        return getIntInRange(props, "max.socket.request.bytes", 100 * 1024 * 1024, 1, Integer.MAX_VALUE);
    }

    /**
     * the number of worker threads that the server uses for handling all
     * client requests
     */
    public int getNumThreads() {
        return getIntInRange(props, "num.threads", Runtime.getRuntime().availableProcessors(), 1, Integer.MAX_VALUE);
    }

    /**
     * the interpublic String get in which to measure performance
     * statistics
     */
    public int getMonitoringPeriodSecs() {
        return getIntInRange(props, "monitoring.period.secs", 600, 1, Integer.MAX_VALUE);
    }

    /**
     * the default number of log partitions per topic
     */
    public int getNumPartitions() {
        return getIntInRange(props, "num.partitions", 1, 1, Integer.MAX_VALUE);
    }

    /**
     * the directory in which the log data is kept
     */
    public String getLogDir() {
        return getString(props, "log.dir");
    }

    /**
     * the maximum size of a single log file
     */
    public int getLogFileSize() {
        return getIntInRange(props, "log.file.size", 1 * 1024 * 1024 * 1024, Message.MinHeaderSize, Integer.MAX_VALUE);
    }

    /**
     * the number of messages accumulated on a log partition before
     * messages are flushed to disk
     */
    public int getFlushInterval() {
        return getIntInRange(props, "log.flush.interval", 500, 1, Integer.MAX_VALUE);
    }

    /**
     * the number of hours to keep a log file before deleting it
     */
    public int getLogRetentionHours() {
        return getIntInRange(props, "log.retention.hours", 24 * 7, 1, Integer.MAX_VALUE);
    }

    /**
     * the maximum size of the log before deleting it
     */
    public int getLogRetentionSize() {
        return getInt(props, "log.retention.size", -1);
    }

    /**
     * the number of hours to keep a log file before deleting it for some
     * specific topic
     */
    public Map<String, Integer> getLogRetentionHoursMap() {
        return getTopicRentionHours(getString(props, "topic.log.retention.hours", ""));
    }

    /**
     * the frequency in minutes that the log cleaner checks whether any log
     * is eligible for deletion
     */
    public int getLogCleanupIntervalMinutes() {
        return getIntInRange(props, "log.cleanup.interval.mins", 10, 1, Integer.MAX_VALUE);
    }

    /**
     * enable zookeeper registration in the server
     */
    public boolean getEnableZookeeper() {
        return getBoolean(props, "enable.zookeeper", false);
    }

    /**
     * the maximum time in ms that a message in selected topics is kept in
     * memory before flushed to disk, e.g., topic1:3000,topic2: 6000
     */
    public Map<String, Integer> getFlushIntervalMap() {
        return getTopicFlushIntervals(getString(props, "topic.flush.intervals.ms", ""));
    }

    /**
     * the frequency in ms that the log flusher checks whether any log
     * needs to be flushed to disk
     */
    public int getFlushSchedulerThreadRate() {
        return getInt(props, "log.default.flush.scheduler.interval.ms", 3000);
    }

    /**
     * the maximum time in ms that a message in any topic is kept in memory
     * before flushed to disk
     */
    public int getDefaultFlushIntervalMs() {
        return getInt(props, "log.default.flush.interval.ms", getFlushSchedulerThreadRate());
    }

    /**
     * the number of partitions for selected topics, e.g.,
     * topic1:8,topic2:16
     */
    public Map<String, Integer> getTopicPartitionsMap() {
        return getTopicPartitions(getString(props, "topic.partition.count.map", ""));
    }

    /**
     * get the rolling strategy (default value is
     * {@link FixedSizeRollingStrategy})
     *
     * @return RollingStrategy Object
     */
    public RollingStrategy getRollingStrategy() {
        return Utils.getObject(getString(props, "log.rolling.strategy", null));
    }

    /**
     * get Authentication method
     *
     * @return Authentication method
     * @see Authentication#build(String)
     */
    public Authentication getAuthentication() {
        return authentication;
    }

    /**
     * maximum size of message that the server can receive (default 1MB)
     *
     * @return maximum size of message
     */
    public int getMaxMessageSize() {
        return getIntInRange(props, "max.message.size", 1024 * 1024, 0, Integer.MAX_VALUE);
    }
}
