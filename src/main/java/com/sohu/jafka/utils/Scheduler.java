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

package com.sohu.jafka.utils;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/**
 * A scheduler for running jobs in the background
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
public class Scheduler {

    final private Logger logger = Logger.getLogger(Scheduler.class);

    final AtomicLong threadId = new AtomicLong(0);

    final ScheduledThreadPoolExecutor executor;

    final String baseThreadName;

    /**
     * create a scheduler
     * 
     * @param numThreads
     * @param baseThreadName
     * @param isDaemon
     */
    public Scheduler(int numThreads, final String baseThreadName, final boolean isDaemon) {
        this.baseThreadName = baseThreadName;
        executor = new ScheduledThreadPoolExecutor(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, baseThreadName + threadId.getAndIncrement());
                t.setDaemon(isDaemon);
                return t;
            }
        });
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public ScheduledFuture<?> scheduleWithRate(Runnable command, long delayMs, long periodMs) {
        return executor.scheduleAtFixedRate(command, delayMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public void shutdownNow() {
        executor.shutdownNow();
        logger.info("force shutdown scheduler " + baseThreadName);
    }

    public void shutdown() {
        executor.shutdown();
        logger.info("shutdown scheduler " + baseThreadName);
    }
}
