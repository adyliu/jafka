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

package com.sohu.jafka.cluster;

import com.sohu.jafka.server.ServerRegister;

/**
 * messages store broker
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Broker {

    /**
     * the global unique broker id
     */
    public final int id;

    /**
     * the broker creator (hostname with created time)
     *
     * @see ServerRegister#registerBrokerInZk()
     */
    public final String creatorId;

    /**
     * broker hostname
     */
    public final String host;

    /**
     * broker port
     */
    public final int port;

    /**
     * create a broker
     *
     * @param id        broker id
     * @param creatorId the creator id
     * @param host      broker hostname
     * @param port      broker port
     */
    public Broker(int id, String creatorId, String host, int port) {
        super();
        this.id = id;
        this.creatorId = creatorId;
        this.host = host;
        this.port = port;
    }

    /**
     * the broker info saved in zookeeper
     * <p>
     * format: <b>creatorId:host:port</b>
     * </p>
     *
     * @return broker info saved in zookeeper
     */
    public String getZKString() {
        StringBuilder buf = new StringBuilder(32);
        buf.append(creatorId.replace(':','#'));
        buf.append(':');
        buf.append(host.replace(':','#'));
        buf.append(':');
        buf.append(port);
        return buf.toString();
    }

    @Override
    public String toString() {
        return "id:" + id + ",creatorId:" + creatorId + ",host:" + host + ",port:" + port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + id;
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Broker other = (Broker) obj;
        if (host == null) {
            if (other.host != null) return false;
        } else if (!host.equals(other.host)) return false;
        if (id != other.id) return false;
        if (port != other.port) return false;
        return true;
    }

    /**
     * create a broker with given broker info
     *
     * @param id               broker id
     * @param brokerInfoString broker info format: <b>creatorId:host:port</b>
     * @return broker instance with connection config
     * @see #getZKString()
     */
    public static Broker createBroker(int id, String brokerInfoString) {
        String[] brokerInfo = brokerInfoString.split(":");
        String creator = brokerInfo[0].replace('#', ':');
        String hostname = brokerInfo[1].replace('#', ':');
        String port = brokerInfo[2];
        return new Broker(id, creator, hostname, Integer.parseInt(port));
    }

}
