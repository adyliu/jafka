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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SegmentListTest {

    private SegmentList createSegmentList(int count) {
        List<LogSegment> segments = new ArrayList<LogSegment>();
        for (int i = 0; i < count; i++) {
            segments.add(new LogSegment(null, null, i));
        }
        final SegmentList segmentList = new SegmentList("demo", segments);
        return segmentList;
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.log.SegmentList#append(com.sohu.jafka.log.LogSegment)}.
     */
    @Test
    public void testAppend() throws Exception {
        final int count = 100;
        final SegmentList segmentList = createSegmentList(count);
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread() {

                public void run() {
                    try {
                        segmentList.append(new LogSegment(null, null, 0));
                    } finally {
                        latch.countDown();
                    }
                }
            }.start();
        }
        latch.await();
        assertEquals(count + count, segmentList.getView().size());
    }

    /**
     * Test method for {@link com.sohu.jafka.log.SegmentList#trunc(int)}.
     */
    @Test
    public void testTrunc() throws Exception {
        final int count = 100;
        final int del = 2;
        final SegmentList segmentList = createSegmentList(count * (del + 1));
        final int totalCount = segmentList.getView().size();
        final CountDownLatch latch = new CountDownLatch(count);
        final AtomicInteger deletedCount = new AtomicInteger();
        for (int i = 0; i < count; i++) {
            new Thread() {

                public void run() {
                    try {
                        deletedCount.addAndGet(segmentList.trunc(del).size());
                    } finally {
                        latch.countDown();
                    }
                }
            }.start();
        }
        latch.await();
        assertEquals(totalCount - deletedCount.get(), segmentList.getView().size());
        assertEquals(count * del, deletedCount.get());
    }

    /**
     * Test method for {@link com.sohu.jafka.log.SegmentList#getLastView()}.
     */
    @Test
    public void testGetLastView() {
        final SegmentList segmentList = createSegmentList(10);
        assertEquals(9, segmentList.getLastView().start());
    }

    /**
     * Test method for {@link com.sohu.jafka.log.SegmentList#getView()}.
     */
    @Test
    public void testGetView() {
        final int count = 100;
        final SegmentList segmentList = createSegmentList(count);
        List<LogSegment> segments = segmentList.getView();
        assertEquals(count, segments.size());
        for (int i = 0; i < count; i++) {
            assertEquals(i, segments.get(i).start());
        }
    }

}
