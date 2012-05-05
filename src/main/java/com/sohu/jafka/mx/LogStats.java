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

import java.util.concurrent.atomic.AtomicLong;

import com.sohu.jafka.log.Log;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class LogStats implements LogStatsMBean, IMBeanName {

    final Log log;

    private final AtomicLong numCumulatedMessages = new AtomicLong(0);

    private String mbeanName;

    /**
     * @param log
     */
    public LogStats(Log log) {
        this.log = log;
    }

    /**
     * @return the mbeanName
     */
    public String getMbeanName() {
        return mbeanName;
    }

    /**
     * @param mbeanName the mbeanName to set
     */
    public void setMbeanName(String mbeanName) {
        this.mbeanName = mbeanName;
    }

    public String getName() {
        return log.name;
    }

    public long getSize() {
        return log.size();
    }

    public int getNumberOfSegments() {
        return log.getNumberOfSegments();
    }

    public long getCurrentOffset() {
        return log.getHighwaterMark();
    }

    public long getNumAppendedMessages() {
        return numCumulatedMessages.get();
    }

    public void recordAppendedMessages(int nMessages) {
        numCumulatedMessages.getAndAdd(nMessages);
    }

}
