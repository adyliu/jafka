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

import com.sohu.jafka.utils.Pool;
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class BrokerTopicStat implements BrokerTopicStatMBean, IMBeanName {

    static class BrokerTopicStatHolder {

        static BrokerTopicStat allTopicState = new BrokerTopicStat();

        static Pool<String, BrokerTopicStat> states = new Pool<String, BrokerTopicStat>();
        static {
            allTopicState.mBeanName = "jafka:type=jafka.BrokerAllTopicStat";
            Utils.registerMBean(allTopicState);
        }
    }

    public static BrokerTopicStat getBrokerAllTopicStat() {
        return BrokerTopicStatHolder.allTopicState;
    }

    public static BrokerTopicStat getBrokerTopicStat(String topic) {
        return getInstance(topic);
    }

    public static BrokerTopicStat getInstance(String topic) {
        BrokerTopicStat state = BrokerTopicStatHolder.states.get(topic);
        if (state == null) {
            state = new BrokerTopicStat();
            state.mBeanName = "jafka:type=jafka.BrokerTopicStat." + topic;
            if (null == BrokerTopicStatHolder.states.putIfNotExists(topic, state)) {
                Utils.registerMBean(state);
            }
            state = BrokerTopicStatHolder.states.get(topic);
        }
        return state;
    }

    private String mBeanName;

    //

    private final AtomicLong numCumulatedBytesIn = new AtomicLong(0);

    private final AtomicLong numCumulatedBytesOut = new AtomicLong(0);

    private final AtomicLong numCumulatedFailedFetchRequests = new AtomicLong(0);

    private final AtomicLong numCumulatedFailedProduceRequests = new AtomicLong(0);

    private final AtomicLong numCumulatedMessagesIn = new AtomicLong(0);

    private BrokerTopicStat() {
    }

    public long getBytesIn() {
        return numCumulatedBytesIn.get();
    }

    public long getBytesOut() {
        return numCumulatedBytesOut.get();
    }

    public long getFailedFetchRequest() {
        return numCumulatedFailedFetchRequests.get();
    }

    public long getFailedProduceRequest() {
        return numCumulatedFailedProduceRequests.get();
    }

    public String getMbeanName() {
        return mBeanName;
    }

    public long getMessagesIn() {
        return numCumulatedMessagesIn.get();
    }

    public void recordBytesIn(long nBytes) {
        numCumulatedBytesIn.getAndAdd(nBytes);
    }

    public void recordBytesOut(long nBytes) {
        numCumulatedBytesOut.getAndAdd(nBytes);
    }

    public void recordFailedFetchRequest() {
        numCumulatedFailedFetchRequests.getAndIncrement();
    }

    public void recordFailedProduceRequest() {
        numCumulatedFailedProduceRequests.getAndIncrement();
    }

    public void recordMessagesIn(int nMessages) {
        numCumulatedBytesIn.getAndAdd(nMessages);
    }
}
