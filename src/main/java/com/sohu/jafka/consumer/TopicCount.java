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

package com.sohu.jafka.consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class TopicCount {

    private final String consumerIdString;

    private final Map<String, Integer> topicCountMap;

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * @param consumerIdString groupid-consumerid
     * @param topicCountMap map: topic->threadCount
     */
    public TopicCount(String consumerIdString, Map<String, Integer> topicCountMap) {
        this.consumerIdString = consumerIdString;
        this.topicCountMap = topicCountMap;
    }
    /**
     * 
     * @return topic->(consumerIdString-0,consumerIdString-1..)
     */
    public Map<String, Set<String>> getConsumerThreadIdsPerTopic() {
        Map<String, Set<String>> consumerThreadIdsPerTopicMap = new HashMap<String, Set<String>>();
        for (Map.Entry<String, Integer> e : topicCountMap.entrySet()) {
            Set<String> consumerSet = new HashSet<String>();
            final int nCounsumers = e.getValue().intValue();
            for (int i = 0; i < nCounsumers; i++) {
                consumerSet.add(consumerIdString + "-" + i);
            }
            consumerThreadIdsPerTopicMap.put(e.getKey(), consumerSet);
        }
        return consumerThreadIdsPerTopicMap;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerIdString == null) ? 0 : consumerIdString.hashCode());
        result = prime * result + ((topicCountMap == null) ? 0 : topicCountMap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TopicCount other = (TopicCount) obj;
        if (consumerIdString == null) {
            if (other.consumerIdString != null) return false;
        } else if (!consumerIdString.equals(other.consumerIdString)) return false;
        if (topicCountMap == null) {
            if (other.topicCountMap != null) return false;
        } else if (!topicCountMap.equals(other.topicCountMap)) return false;
        return true;
    }
    /**
     * topic->count map
     * @return json map
     */
    public String toJsonString() {
        try {
            return mapper.writeValueAsString(topicCountMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "TopicCount [consumerIdString=" + consumerIdString + ", topicCountMap=" + topicCountMap + "]";
    }

    public static TopicCount parse(String consumerIdString, String jsonString) {
        try {
            Map<String, Integer> topicCountMap = mapper.readValue(jsonString, new TypeReference<Map<String, Integer>>() {});
            return new TopicCount(consumerIdString, topicCountMap);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("error parse consumer json string " + jsonString, e);
        } catch (JsonMappingException e) {
            throw new IllegalArgumentException("error parse consumer json string " + jsonString, e);
        } catch (IOException e) {
            throw new IllegalArgumentException("error parse consumer json string " + jsonString, e);
        }
    }

}
