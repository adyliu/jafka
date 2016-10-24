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
 * @since 1.0
 */
public interface Range {

    /** The first index in the range
     * @return range of start
     */
    long start();

    /** The total number of indexes in the range
     * @return size of range
     */
    long size();

    /** Return true iff the range is empty
     * @return check the range is emtpy
     */
    boolean isEmpty();

    /** if value is in range
     * @param value value for check
     * @return check the value in range
     */
    boolean contains(long value);

    String toString();

}
