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

package com.sohu.jafka.network;


import java.util.ArrayList;
import java.util.List;

import com.sohu.jafka.common.annotations.NotThreadSafe;

/**
 * A set of message sets prefixed by size
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@NotThreadSafe
public class MultiMessageSetSend extends MultiSend<Send> {

    public MultiMessageSetSend(List<MessageSetSend> sets) {
        super();
        final ByteBufferSend sizeBuffer = new ByteBufferSend(6);
        List<Send> sends = new ArrayList<Send>(sets.size() + 1);
        sends.add(sizeBuffer);
        int allMessageSetSize = 0;
        for (MessageSetSend send : sets) {
            sends.add(send);
            allMessageSetSize += send.getSendSize();
        }
        //write head size
        sizeBuffer.getBuffer().putInt(2 + allMessageSetSize);//4
        sizeBuffer.getBuffer().putShort((short) 0);//2
        sizeBuffer.getBuffer().rewind();
        super.expectedBytesToWrite = 4 + 2 + allMessageSetSize;
        //
        super.setSends(sends);
    }

}
