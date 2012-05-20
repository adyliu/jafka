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

package com.sohu.jafka.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.MessageSet;

/**
 * log interface
 * <p>
 * A log describes a topic with partition(default value is 0).
 * </p>
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public interface ILog extends Closeable {

    List<Long> EMPTY_OFFSETS = Collections.emptyList();

    /**
     * read messages from log
     * 
     * @param offset offset of messages
     * @param length the max messages size
     * @return message objects
     * @throws IOException
     */
    MessageSet read(long offset, int length) throws IOException;

    /**
     * append messages to log
     * 
     * @param messages message set
     * @return all message offsets or null if not supported
     */
    List<Long> append(ByteBufferMessageSet messages);

    /**
     * get offsets of before the OffsetRequest's time
     * 
     * @param offsetRequest offset request
     * @return the offsets earlier than the offset request
     */
    List<Long> getOffsetsBefore(OffsetRequest offsetRequest);
}
