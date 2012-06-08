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

package com.sohu.jafka.admin;

import java.io.IOException;

import com.sohu.jafka.api.CreaterRequest;
import com.sohu.jafka.api.DeleterRequest;
import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.consumer.SimpleOperation;
import com.sohu.jafka.network.Receive;
import com.sohu.jafka.utils.KV;
import com.sohu.jafka.utils.Utils;

/**
 * Some useful tools for server operation
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
public class AdminOperation extends SimpleOperation {

    public AdminOperation(String host, int port) {
        super(host, port);
    }

    /**
     * create partitions in the broker
     * 
     * @param topic topic name
     * @param partitionNum partition numbers
     * @param enlarge enlarge partition number if broker configuration has
     *        setted
     * @return partition number in the broker
     * @throws IOException if an I/O error occurs
     */
    public int createPartitions(String topic, int partitionNum, boolean enlarge) throws IOException {
        KV<Receive, ErrorMapping> response = send(new CreaterRequest(topic, partitionNum, enlarge));
        return Utils.deserializeIntArray(response.k.buffer())[0];
    }

    /**
     * delete topic never used
     * 
     * @param topic topic name
     * @param password password
     * @return number of partitions deleted
     * @throws IOException if an I/O error
     */
    public int deleteTopic(String topic, String password) throws IOException {
        KV<Receive, ErrorMapping> response = send(new DeleterRequest(topic, password));
        return Utils.deserializeIntArray(response.k.buffer())[0];
    }

}
