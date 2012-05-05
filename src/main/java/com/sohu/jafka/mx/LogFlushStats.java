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

import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class LogFlushStats implements LogFlushStatsMBean, IMBeanName {

    private final SnapshotStats flushRequestStats = new SnapshotStats();

    private LogFlushStats() {
    }

    // public void recordFlushRequest(long requestNs) {
    //     flushRequestStats.recordRequestMetric(requestNs);
    //}

    public double getFlushesPerSecond() {
        return flushRequestStats.getRequestsPerSecond();
    }

    public double getAvgFlushMs() {
        return flushRequestStats.getAvgMetric();
    }

    public long getTotalFlushMs() {
        return flushRequestStats.getTotalMetric();
    }

    public double getMaxFlushMs() {
        return flushRequestStats.getMaxMetric();
    }

    public long getNumFlushes() {
        return flushRequestStats.getNumRequests();
    }

    public String getMbeanName() {
        return "jafka:type=jafka.LogFlushStats";
    }

    private static class LogFlushStatsHolder {

        static LogFlushStats stats = new LogFlushStats();
        static {
            Utils.registerMBean(stats);
        }
    }

    public static void recordFlushRequest(long requestMs) {
        LogFlushStatsHolder.stats.flushRequestStats.recordRequestMetric(requestMs);
    }

}
