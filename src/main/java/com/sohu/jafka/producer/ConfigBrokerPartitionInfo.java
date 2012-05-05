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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.sohu.jafka.cluster.Broker;
import com.sohu.jafka.cluster.Partition;
import com.sohu.jafka.common.InvalidConfigException;
import com.sohu.jafka.common.annotations.ClientSide;


/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
public class ConfigBrokerPartitionInfo implements BrokerPartitionInfo {

    private final ProducerConfig producerConfig;

    private final SortedSet<Partition> brokerPartitions;

    private final Map<Integer, Broker> allBrokers;

    public ConfigBrokerPartitionInfo(ProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
        this.brokerPartitions = getConfigTopicPartitionInfo();
        this.allBrokers = getConfigBrokerInfo();
    }

    public SortedSet<Partition> getBrokerPartitionInfo(String topic) {
        return brokerPartitions;
    }

    public Broker getBrokerInfo(int brokerId) {
        return allBrokers.get(brokerId);
    }

    public Map<Integer, Broker> getAllBrokerInfo() {
        return allBrokers;
    }

    public void updateInfo() {
    }

    public void close() {
    }

    private SortedSet<Partition> getConfigTopicPartitionInfo() {
        String[] brokerInfoList = producerConfig.getBrokerList().split(",");
        if (brokerInfoList.length == 0) {
            throw new InvalidConfigException("broker.list is empty");
        }
        final TreeSet<Partition> brokerParts = new TreeSet<Partition>();
        for (String bInfo : brokerInfoList) {
            brokerParts.add(new Partition(Integer.parseInt(bInfo.split(":")[0]), 0));
        }
        return brokerParts;
    }

    private Map<Integer, Broker> getConfigBrokerInfo() {
        Map<Integer, Broker> brokerInfo = new HashMap<Integer, Broker>();
        String[] brokerInfoList = producerConfig.getBrokerList().split(",");
        for (String bInfo : brokerInfoList) {
            final String[] idHostPort = bInfo.split(":");
            final int id = Integer.parseInt(idHostPort[0]);
            brokerInfo.put(id, new Broker(id, idHostPort[1], idHostPort[1], Integer.parseInt(idHostPort[2])));
        }
        return brokerInfo;
    }
}
