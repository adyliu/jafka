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

import com.sohu.jafka.api.RequestKeys;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SocketServerStats implements SocketServerStatsMBean, IMBeanName {

    final long monitorDurationNs;

    //=======================================
    final SnapshotStats produceTimeStats;

    final SnapshotStats fetchTimeStats;

    final SnapshotStats produceBytesStats;

    final SnapshotStats fetchBytesStats;

    //=======================================
    public SocketServerStats(long monitorDurationNs) {
        this.monitorDurationNs = monitorDurationNs;
        produceTimeStats = new SnapshotStats(monitorDurationNs);
        fetchTimeStats = new SnapshotStats(monitorDurationNs);
        produceBytesStats = new SnapshotStats(monitorDurationNs);
        fetchBytesStats = new SnapshotStats(monitorDurationNs);
    }

    public void recordBytesRead(int bytes) {
        produceBytesStats.recordRequestMetric(bytes);
    }

    public void recordRequest(RequestKeys requestTypeId, long durationNs) {
        switch (requestTypeId) {
            case PRODUCE:
            case MULTIPRODUCE:
                produceTimeStats.recordRequestMetric(durationNs);
                break;
            case FETCH:
            case MULTIFETCH:
                fetchTimeStats.recordRequestMetric(durationNs);
            default:
                break;
        }
    }

    public void recordBytesWritten(int bytes) {
        fetchBytesStats.recordRequestMetric(bytes);
    }

    public double getProduceRequestsPerSecond() {
        return produceTimeStats.getRequestsPerSecond();
    }

    public double getFetchRequestsPerSecond() {
        return fetchTimeStats.getRequestsPerSecond();
    }

    public double getAvgProduceRequestMs() {
        return produceTimeStats.getAvgMetric() / (1000.0 * 1000.0);
    }

    public double getMaxProduceRequestMs() {
        return produceTimeStats.getMaxMetric() / (1000.0 * 1000.0);
    }

    public double getAvgFetchRequestMs() {
        return fetchTimeStats.getAvgMetric() / (1000.0 * 1000.0);
    }

    public double getMaxFetchRequestMs() {
        return fetchTimeStats.getMaxMetric() / (1000.0 * 1000.0);
    }

    public double getBytesReadPerSecond() {
        return produceBytesStats.getAvgMetric();
    }

    public double getBytesWrittenPerSecond() {
        return fetchBytesStats.getAvgMetric();
    }

    public long getNumFetchRequests() {
        return fetchTimeStats.getNumRequests();
    }

    public long getNumProduceRequests() {
        return produceTimeStats.getNumRequests();
    }

    public long getTotalBytesRead() {
        return produceBytesStats.getTotalMetric();
    }

    public long getTotalBytesWritten() {
        return fetchBytesStats.getTotalMetric();
    }

    public long getTotalFetchRequestMs() {
        return fetchTimeStats.getTotalMetric();
    }

    public long getTotalProduceRequestMs() {
        return produceTimeStats.getTotalMetric();
    }

    public String getMbeanName() {
        return "jafka:type=jafka.SocketServerStats";
    }
}
