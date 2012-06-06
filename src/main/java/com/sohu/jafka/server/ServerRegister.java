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

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNodeExistsException;
import com.sohu.jafka.cluster.Broker;
import com.sohu.jafka.log.LogManager;
import com.sohu.jafka.server.TopicTask.TaskType;
import com.sohu.jafka.utils.zookeeper.ZKStringSerializer;
import com.sohu.jafka.utils.zookeeper.ZkUtils;

/**
 * Handles the server's interaction with zookeeper. The server needs to
 * register the following paths:
 * 
 * <pre>
 *   /topics/[topic]/[node_id-partition_num]
 *   /brokers/[0...N] --> host:port
 * </pre>
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ServerRegister implements IZkStateListener, Closeable {

    private final ServerConfig config;

    private final LogManager logManager;

    private static final Logger logger = Logger.getLogger(ServerRegister.class);

    private final String brokerIdPath;

    private ZkClient zkClient;

    private Set<String> topics = new LinkedHashSet<String>();

    private final Object lock = new Object();

    public ServerRegister(ServerConfig config, LogManager logManager) {
        this.config = config;
        this.logManager = logManager;
        //
        this.brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.getBrokerId();
    }

    public void startup() {
        logger.info("connecting to zookeeper: " + config.getZkConnect());
        zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(), config.getZkConnectionTimeoutMs(), ZKStringSerializer.getInstance());
        zkClient.subscribeStateChanges(this);
    }

    public void processTask(TopicTask task) {
        final String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + task.topic + "/" + config.getBrokerId();
        synchronized (lock) {
            switch (task.type) {
                case DELETE:
                    ZkUtils.deletePath(zkClient, brokerTopicPath);
                    topics.remove(task.topic);
                    break;
                case CREATE:
                    ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, "" + getPartitions(task.topic));
                    break;
                case ENLARGE:
                    ZkUtils.deletePath(zkClient, brokerTopicPath);
                    ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, "" + getPartitions(task.topic));
                    break;
                default:
                    logger.error("unknow task: "+task);
                    break;
            }
        }
    }

    private int getPartitions(String topic) {
        Integer numParts = logManager.getTopicPartitionsMap().get(topic);
        return numParts == null ? config.getNumPartitions() : numParts.intValue();
    }

    /**
     * register broker in the zookeeper
     * <p>
     * path: /brokers/ids/<id> <br/>
     * data: creator:host:port
     * </p>
     */
    public void registerBrokerInZk() {
        logger.info("Registering broker " + brokerIdPath);
        String hostname = config.getHostName();
        if (hostname == null) {
            try {
                hostname = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException("cannot get local host, setting 'hostname' in configuration");
            }
        }
        //
        final String creatorId = hostname + "-" + System.currentTimeMillis();
        final Broker broker = new Broker(config.getBrokerId(), creatorId, hostname, config.getPort());
        try {
            ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString());
        } catch (ZkNodeExistsException e) {
            throw new RuntimeException("A broker is already registered on the path " + brokerIdPath + ". This probably "
                    + "indicates that you either have configured a brokerid that is already in use, or "
                    + "else you have shutdown this broker and restarted it faster than the zookeeper " + "timeout so it appears to be re-registering.");
        }
        //
        logger.info("Registering broker " + brokerIdPath + " succeeded with " + broker);
    }

    /**
     * 
     */
    public void close() {
        if (zkClient != null) {
            logger.info("closing zookeeper client...");
            zkClient.close();
        }
    }

    public void handleNewSession() throws Exception {
        logger.info("re-registering broker info in zookeeper for broker " + config.getBrokerId());
        registerBrokerInZk();
        synchronized (lock) {
            logger.info("re-registering broker topics in zookeeper for broker " + config.getBrokerId());
            for (String topic : topics) {
                processTask(new TopicTask(TaskType.CREATE, topic));
            }
        }

    }

    public void handleStateChanged(KeeperState state) throws Exception {
        // do nothing, since zkclient will do reconnect for us.
    }
}
