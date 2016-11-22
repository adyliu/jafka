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

package io.jafka.api;

import io.jafka.common.annotations.ClientSide;
import io.jafka.common.annotations.ServerSide;

/**
 * Request Type
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
@ServerSide
public enum RequestKeys {
    PRODUCE, //0
    FETCH, //1
    MULTIFETCH, //2
    MULTIPRODUCE, //3
    OFFSETS,//4
    /** create more partitions
     * @since 1.2
     */
    CREATE,//5
    /**
     * delete unused topic
     * @since 1.2
     */
    DELETE;//6

    //
    public int value = ordinal();

    //
    final static int size = values().length;

    public static RequestKeys valueOf(int ordinal) {
        if (ordinal < 0 || ordinal >= size) return null;
        return values()[ordinal];
    }
}
