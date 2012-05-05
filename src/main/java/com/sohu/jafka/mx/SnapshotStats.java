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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * statistic tools
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SnapshotStats {

    //final Time time = com.sohu.jafka.utils.Time.SystemTime;

    private final long monitorDurationNs;

    private final AtomicReference<Stats> complete = new AtomicReference<Stats>(new Stats());

    private final AtomicReference<Stats> current = new AtomicReference<Stats>(new Stats());

    private final AtomicLong total = new AtomicLong(0);

    private final AtomicLong numCumulatedRequests = new AtomicLong(0);

    public SnapshotStats(long monitorDurationNs) {
        this.monitorDurationNs = monitorDurationNs;
    }

    public SnapshotStats() {
        this(TimeUnit.MINUTES.toNanos(10));
    }

    public void recordRequestMetric(long requestNs) {
        Stats stats = current.get();
        stats.add(requestNs);
        total.getAndAdd(requestNs);
        numCumulatedRequests.getAndAdd(1);
        long ageNs = System.nanoTime() - stats.start;
        // if the current stats are too old it is time to swap
        if (ageNs >= monitorDurationNs) {
            boolean swapped = current.compareAndSet(stats, new Stats());
            if (swapped) {
                complete.set(stats);
                stats.end.set(System.nanoTime());
            }
        }
    }

    public void recordThroughputMetric(long data) {
        Stats stats = current.get();
        stats.addData(data);
        long ageNs = System.nanoTime() - stats.start;
        // if the current stats are too old it is time to swap
        if (ageNs >= monitorDurationNs) {
            boolean swapped = current.compareAndSet(stats, new Stats());
            if (swapped) {
                complete.set(stats);
                stats.end.set(System.nanoTime());
            }
        }
    }

    public long getNumRequests() {
        return numCumulatedRequests.get();
    }

    public double getRequestsPerSecond() {
        Stats stats = complete.get();
        return stats.numRequests / stats.durationSeconds();
    }

    public double getThroughput() {
        Stats stats = complete.get();
        return stats.totalData / stats.durationSeconds();
    }

    public double getAvgMetric() {
        Stats stats = complete.get();
        if (stats.numRequests == 0) {
            return 0;
        } else {
            return stats.totalRequestMetric / stats.numRequests;
        }
    }

    public long getTotalMetric() {
        return total.get();
    }

    public double getMaxMetric() {
        return complete.get().maxRequestMetric;
    }

    //================================================================
    class Stats {

        final long start = System.nanoTime();

        static final double SECOND2NANO = 1000.0 * 1000.0 * 1000.0;

        static final double MILLISECOND2NANO = 1000.0 * 1000.0;

        AtomicLong end = new AtomicLong(-1);

        int numRequests = 0;

        long totalRequestMetric = 0L;

        long maxRequestMetric = 0L;

        long totalData = 0L;

        private Object lock = new Object();

        long addData(long data) {
            synchronized (lock) {
                totalData += data;
                return totalData;
            }
        }

        long add(long requestNs) {
            synchronized (lock) {
                numRequests += 1;
                totalRequestMetric += requestNs;
                maxRequestMetric = Math.max(maxRequestMetric, requestNs);
                return maxRequestMetric;
            }
        }

        double durationSeconds() {
            return (end.get() - start) / SECOND2NANO;
        }

        double durationMs() {
            return (end.get() - start) / MILLISECOND2NANO;
        }
    }
}
