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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.api.MultiFetchResponse;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.message.ByteBufferMessageSet;

/**
 * Consumer Interface
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.1
 */
public interface IConsumer extends Closeable {

    /**
     * fetch some messages from the broker
     * 
     * @param request request arguments
     * @return bytebuffer message
     * @throws IOException if an I/O error occurs
     * @see FetchRequest
     */
    ByteBufferMessageSet fetch(FetchRequest request) throws IOException;

    /**
     * get before offsets of message
     * 
     * @param topic message topic
     * @param partition topic partition
     * @param time the log file created time {@link OffsetRequest#time}
     * @param maxNumOffsets the number of offsets
     * @return offsets while file created before the time or empty offsets
     * @throws IOException if an I/O error occurs
     */
    long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) throws IOException;

    /**
     * get the latest offset
     * 
     * @param topic topic name
     * @param partition partition index
     * @return latest offset if topic exist; -1 otherwise
     * @throws IOException if an I/O error occurs
     */
    long getLatestOffset(String topic, int partition) throws IOException;

    /**
     * fetch some topic with given arguments
     * 
     * @param fetches fetching arguments
     * @return multi bytebuffer messages
     * @throws IOException if an I/O error occurs
     * @see #fetch(FetchRequest)
     * @see FetchRequest
     */
    MultiFetchResponse multifetch(List<FetchRequest> fetches) throws IOException;

    /**
     * close the connection
     */
    void close() throws IOException;
}
