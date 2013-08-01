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

import com.sohu.jafka.message.FileMessageSet;
import com.sohu.jafka.utils.Range;

import java.io.File;

/**
 * A segment with a file would store the log message
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class LogSegment implements Range, Comparable<LogSegment> {

    private final File file;

    private final FileMessageSet messageSet;

    private final long start;

    private volatile boolean deleted;

    public LogSegment(File file, FileMessageSet messageSet, long start) {
        super();
        this.file = file;
        this.messageSet = messageSet;
        this.start = start;
        this.deleted = false;
    }

    public long start() {
        return start;
    }

    /**
     * @return the deleted
     */
    public boolean isDeleted() {
        return deleted;
    }

    public File getFile() {
        return file;
    }

    public FileMessageSet getMessageSet() {
        return messageSet;
    }

    /**
     * @param deleted the deleted to set
     */
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    /**
     * persistence size
     */
    public long size() {
        return messageSet.highWaterMark();
    }

    /**
     * addressing size is larger than {@link #size()} while some messages unflushed to disk
     *
     * @return the addressing size
     */
    public long addressingSize() {
        return messageSet.getSizeInBytes();
    }

    public int compareTo(LogSegment o) {
        return this.start > o.start ? 1 : this.start < o.start ? -1 : 0;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean contains(long value) {
        long size = size();
        long start = start();
        return ((size == 0 && value == start) //
                || (size > 0 && value >= start && value <= start + size - 1));
    }

    @Override
    public String toString() {
        return "(file=" + file + ", start=" + start + ", size=" + size() + ")";
    }

    public String getName() {
        return file.getName();
    }
}
