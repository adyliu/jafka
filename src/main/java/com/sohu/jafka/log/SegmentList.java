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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-6
 */
public class SegmentList {

    final AtomicReference<List<LogSegment>> contents;

    /**
     * @param accum
     */

    public SegmentList(List<LogSegment> accum) {
        contents = new AtomicReference<List<LogSegment>>(accum);
    }

    /**
     * Append the given items to the end of the list
     */
    public void append(LogSegment... ts) {
        while (true) {
            List<LogSegment> curr = contents.get();
            List<LogSegment> updated = new ArrayList<LogSegment>(curr.size() + ts.length);
            updated.addAll(curr);
            updated.addAll(Arrays.asList(ts));
            if (contents.compareAndSet(curr, updated)) {
                return;
            }
        }
    }

    /**
     * Delete the first n items from the list
     */
    public List<LogSegment> trunc(int newStart) {
        if (newStart < 0) {
            throw new IllegalArgumentException("Starting index must be positive.");
        }
        while (true) {
            List<LogSegment> curr = contents.get();
            int newLength = Math.max(curr.size() - newStart, 0);
            List<LogSegment> updatedList = new ArrayList<LogSegment>(curr.subList(Math.min(newStart, curr.size() - 1), curr.size()));
            if (contents.compareAndSet(curr, updatedList)) {
                return curr.subList(0, curr.size() - newLength);
            }
        }
    }

    public LogSegment getLastView() {
        List<LogSegment> views = getView();
        return views.get(views.size() - 1);
    }

    public List<LogSegment> getView() {
        return contents.get();
    }

    public static final int MaxAttempts = 20;

}
