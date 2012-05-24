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

import java.text.SimpleDateFormat;
import java.util.Date;
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

    public LogStats(Log log) {
        this.log = log;
    }

    public String getMbeanName() {
        return mbeanName;
    }

    public void setMbeanName(String mbeanName) {
        this.mbeanName = mbeanName;
    }

    public String getName() {
        return log.name;
    }

    public void recordAppendedMessages(int nMessages) {
        numCumulatedMessages.getAndAdd(nMessages);
    }

    @Override
    public int getSegmentNum() {
        return log.getNumberOfSegments();
    }

    @Override
    public long getStartingAppendedMessagesNum() {
        return numCumulatedMessages.get();
    }

    @Override
    public long getLastSegmentSize() {
        return log.getHighwaterMark();
    }

    @Override
    public long getLastSegmentAddressingSize() {
        return log.getLastSegmentAddressingSize();
    }

    @Override
    public long getTotalSegmentSize() {
        return log.size();
    }

    @Override
    public long getTotalOffset() {
        return log.getTotalOffset();
    }

    @Override
    public String getLastFlushedTime() {
        long time = log.getLastFlushedTime();
        if (time == 0) return "--";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(new Date(time));
    }
}
