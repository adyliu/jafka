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

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author adyliu (imxylz@gmail.com)
 */
public class PartitionTest {

    /**
     * Test method for
     * {@link com.sohu.jafka.cluster.Partition#compareTo(com.sohu.jafka.cluster.Partition)}
     * .
     */
    @Test
    public void testCompareTo() {
        Partition p1 = new Partition(1, 0);
        Partition p2 = new Partition(2, 2);
        Partition p3 = new Partition(1, 1);
        Partition p4 = new Partition(1, 2);
        Partition p5 = new Partition(2, 0);
        Partition p6 = new Partition(2, 1);
        Partition p7 = new Partition(1, 3);
        List<Partition> partitions = Arrays.asList(p1, p2, p3, p4, p5, p6, p7);
        TreeSet<Partition> set = new TreeSet<Partition>(partitions);
        System.out.println(set);
        Partition[] expectPartitions = {p1,p3,p4,p7,p5,p6,p2};
        Assert.assertArrayEquals(expectPartitions, set.toArray());
        Assert.assertFalse(set.add(new Partition(2,1)));
    }

}
