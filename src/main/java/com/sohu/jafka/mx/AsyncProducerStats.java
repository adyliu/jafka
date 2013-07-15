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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class AsyncProducerStats implements AsyncProducerStatsMBean, IMBeanName {

    private final AtomicInteger droppedEvents = new AtomicInteger();

    private final AtomicInteger numEvents = new AtomicInteger();

    public int getAsyncProducerEvents() {
        return numEvents.get();
    }

    public int getAsyncProducerDroppedEvents() {
        return droppedEvents.get();
    }

    public String getMbeanName() {
        return "jafka.producer.Producer:type=AsyncProducerStats";
    }

    private static class AsyncProducerStatsHolder {

        static AsyncProducerStats instance = new AsyncProducerStats();

        static {
            Utils.registerMBean(instance);
        }
    }

    public static void recordDroppedEvents() {
        AsyncProducerStatsHolder.instance.droppedEvents.incrementAndGet();
    }

    public static void recordEvent() {
        AsyncProducerStatsHolder.instance.numEvents.incrementAndGet();
    }

}
