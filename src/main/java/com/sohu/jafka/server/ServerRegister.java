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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNodeExistsException;
import com.sohu.jafka.cluster.Broker;
import com.sohu.jafka.log.LogManager;
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

    private List<String> topics = new ArrayList<String>();

    private final Object lock = new Object();

    /**
     * @param config
     * @param logManager
     */
    public ServerRegister(ServerConfig config, LogManager logManager) {
        this.config = config;
        this.logManager = logManager;
        //
        this.brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.getBrokerId();
    }

    /**
     * 
     */
    public void startup() {
        logger.info("connecting to zookeeper: " + config.getZkConnect());
        zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(), config.getZkConnectionTimeoutMs(), ZKStringSerializer.getInstance());
        zkClient.subscribeStateChanges(this);
    }

    /**
     * @param topic
     */
    public void registerTopicInZk(String topic) {
        registerTopicInZkInternal(topic);
        synchronized (lock) {
            topics.add(topic);
        }
    }

    private void registerTopicInZkInternal(String topic) {
        final String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic + "/" + config.getBrokerId();
        Integer numParts = logManager.getTopicPartitionsMap().get(topic);
        if (numParts == null) {
            numParts = Integer.valueOf(config.getNumPartitions());
        }
        logger.info("Begin registering broker topic " + brokerTopicPath + " with " + numParts + " partitions");
        ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, "" + numParts);
        logger.info("End registering broker topic " + brokerTopicPath);
    }

    /**
     * 
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
                registerTopicInZkInternal(topic);
            }
        }

    }

    public void handleStateChanged(KeeperState state) throws Exception {
        // do nothing, since zkclient will do reconnect for us.
    }
}
