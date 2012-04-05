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

package com.sohu.jafka.message;

import com.sohu.jafka.common.UnKnownCodecException;

/**
 * message compression method
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-5
 */
public enum CompressionCodec {
    NoCompressionCodec(0), //
    GZIPCompressionCodec(1), //
    SnappyCompressionCodec(2), //
    //
    DefaultCompressionCodec(1);

    public final int codec;

    CompressionCodec(int codec) {
        this.codec = codec;
    }

    //
    public static CompressionCodec valueOf(int codec) {
        switch (codec) {
            case 0:
                return NoCompressionCodec;
            case 1:
                return GZIPCompressionCodec;
            case 2:
                return SnappyCompressionCodec;
        }
        throw new UnKnownCodecException("unkonw codec: " + codec);
    }
}
