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

/**
 * A mockable interface for time functions
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
public interface Time {

    long NsPerUs = 1000L;

    long UsPerMs = 1000L;

    long MsPerSec = 1000L;

    long NsPerMs = NsPerUs * UsPerMs;

    long NsPerSec = NsPerMs * MsPerSec;

    long UsPerSec = UsPerMs * MsPerSec;

    long SecsPerMin = 60;

    long MinsPerHour = 60;

    long HoursPerDay = 24;

    long SecsPerHour = SecsPerMin * MinsPerHour;

    long SecsPerDay = SecsPerHour * HoursPerDay;

    long MinsPerDay = MinsPerHour * HoursPerDay;

    long milliseconds();

    long nanoseconds();

    void sleep(long ms) throws InterruptedException;

    //
    public static final Time SystemTime = new Time() {

        public long milliseconds() {
            return System.currentTimeMillis();
        }

        public long nanoseconds() {
            return System.nanoTime();
        }

        public void sleep(long ms) throws InterruptedException {
            Thread.sleep(ms);
        }
    };
}
