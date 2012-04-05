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

package com.sohu.jafka.utils;

/**
 * A generic range value with a start and end
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
public interface Range {

    /** The first index in the range */
    long start();

    /** The total number of indexes in the range */
    long size();

    /** Return true iff the range is empty */
    boolean isEmpty();

    /** if value is in range */
    boolean contains(long value);

    String toString();

    public static abstract class AbstractRange implements Range {

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
            return "(start=" + start() + ", size=" + size() + ")";
        }
    }
}
