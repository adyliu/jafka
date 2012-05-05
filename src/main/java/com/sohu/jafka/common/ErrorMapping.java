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

package com.sohu.jafka.common;

import java.nio.ByteBuffer;

import com.sohu.jafka.message.InvalidMessageException;


/**
 * A bi-directional mapping between error codes and exceptions x
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public enum ErrorMapping {

    UnkonwCode(-1), //
    NoError(0), //
    OffsetOutOfRangeCode(1), //
    InvalidMessageCode(2), //
    WrongPartitionCode(3), //
    InvalidFetchSizeCode(4);

    public final short code;

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    ErrorMapping(int code) {
        this.code = (short) code;
    }

    public static ErrorMapping valueOf(Exception e) {
        Class<?> clazz = e.getClass();
        if (clazz == OffsetOutOfRangeException.class) {
            return OffsetOutOfRangeCode;
        }
        if (clazz == InvalidMessageException.class) {
            return InvalidMessageCode;
        }
        if (clazz == InvalidPartitionException.class) {
            return WrongPartitionCode;
        }
        if (clazz == InvalidMessageSizeException.class) {
            return InvalidFetchSizeCode;
        }
        return UnkonwCode;
    }

    public static ErrorMapping valueOf(short code) {
        switch (code) {
            case 0:
                return NoError;
            case 1:
                return OffsetOutOfRangeCode;
            case 2:
                return InvalidMessageCode;
            case 3:
                return WrongPartitionCode;
            case 4:
                return InvalidFetchSizeCode;
            default:
                return UnkonwCode;
        }
    }
}
