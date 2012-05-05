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

package com.sohu.jafka.mx;

import com.sohu.jafka.common.annotations.ThreadSafe;
import com.sohu.jafka.utils.Utils;

/**
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ThreadSafe
public class SyncProducerStats implements SyncProducerStatsMBean, IMBeanName {

    private final SnapshotStats produceRequestStats = new SnapshotStats();

    public double getProduceRequestsPerSecond() {
        return produceRequestStats.getRequestsPerSecond();
    }

    public double getAvgProduceRequestMs() {
        return produceRequestStats.getAvgMetric() / (1000.0 * 1000.0);
    }

    public double getMaxProduceRequestMs() {
        return produceRequestStats.getMaxMetric() / (1000.0 * 1000.0);
    }

    public long getNumProduceRequests() {
        return produceRequestStats.getNumRequests();
    }

    public String getMbeanName() {
        return "jafka:type=jafka.jafkaProducerStats";
    }

    private static class SyncProducerStatsHolder {

        static SyncProducerStats instance = new SyncProducerStats();
        static {
            Utils.registerMBean(instance);
        }
    }

    public static void recordProduceRequest(long requestMs) {
        SyncProducerStatsHolder.instance.produceRequestStats.recordRequestMetric(requestMs);
    }
}
