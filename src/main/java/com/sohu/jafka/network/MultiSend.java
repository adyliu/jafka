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

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;

/**
 * A set of composite sends, sent one after another
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public abstract class MultiSend<S extends Send> extends AbstractSend {

    protected int expectedBytesToWrite;

    private int totalWritten = 0;

    private List<S> sends;

    private Iterator<S> iter;

    private S current;

    public MultiSend() {

    }

    public MultiSend(List<S> sends) {
        setSends(sends);
    }


    protected void setSends(List<S> sends) {
        this.sends = sends;
        this.iter = sends.iterator();
        if (iter.hasNext()) {
            this.current = iter.next();
        }
    }

    public List<S> getSends() {
        return sends;
    }

    public boolean complete() {
        if (current != null) return false;
        if (totalWritten != expectedBytesToWrite) {
            logger.error("mismatch in sending bytes over socket; expected: " + expectedBytesToWrite + " actual: " + totalWritten);
        }
        return true;
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = current.writeTo(channel);
        totalWritten += written;
        if (current.complete()) {//move to next element while current element is finished writting
            current = iter.hasNext() ? iter.next() : null;
        }
        return written;
    }
}
