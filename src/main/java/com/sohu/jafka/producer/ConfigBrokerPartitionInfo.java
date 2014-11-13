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
import com.sohu.jafka.common.annotations.ClientSide;

/**
 * send messages with given brokers list
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
public class ConfigBrokerPartitionInfo implements BrokerPartitionInfo {

    private final ProducerConfig producerConfig;

    private final SortedSet<Partition> brokerPartitions = new TreeSet<Partition>();

    private final Map<Integer, Broker> allBrokers = new HashMap<Integer, Broker>();

    public ConfigBrokerPartitionInfo(ProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
        try {
            init();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("illegal broker.list", e);
        }
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
        brokerPartitions.clear();
        allBrokers.clear();
    }

    private void init() {
        String[] brokerInfoList = producerConfig.getBrokerList().split(",");
        for (String bInfo : brokerInfoList) {
            final String[] idHostPort = bInfo.split(":");
            final int id = Integer.parseInt(idHostPort[0]);
            final String host = idHostPort[1];
            final int port = Integer.parseInt(idHostPort[2]);
            final int partitions = idHostPort.length > 3 ? Integer.parseInt(idHostPort[3]) : 1;
            final boolean autocreated = idHostPort.length>4?Boolean.valueOf(idHostPort[4]):true;
            allBrokers.put(id, new Broker(id, host, host, port,autocreated));
            for (int i = 0; i < partitions; i++) {
                this.brokerPartitions.add(new Partition(id, i));
            }
        }
    }
}
