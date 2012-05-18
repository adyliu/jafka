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

/**
 * brokerid-partitionid tuple
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Partition implements Comparable<Partition> {

    public final int brokerId;

    public final int partId;

    public Partition(int brokerId, int partId) {
        this.brokerId = brokerId;
        this.partId = partId;
        this.name = brokerId + "-" + partId;
    }

    public Partition(String name) {
        this(1, 1);
    }

    private final String name;

    /**
     * brokerid with partitionid
     * 
     * @return brokerid-partitionid
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }

    public int compareTo(Partition o) {
        if (this.brokerId == o.brokerId) {
            return this.partId - o.partId;
        }
        return this.brokerId - o.brokerId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + brokerId;
        result = prime * result + partId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Partition other = (Partition) obj;
        if (brokerId != other.brokerId) return false;
        if (partId != other.partId) return false;
        return true;
    }

    public static Partition parse(String s) {
        String[] pieces = s.split("-");
        if (pieces.length != 2) {
            throw new IllegalArgumentException("Expected name in the form x-y.");
        }
        return new Partition(Integer.parseInt(pieces[0]), Integer.parseInt(pieces[1]));
    }
}
