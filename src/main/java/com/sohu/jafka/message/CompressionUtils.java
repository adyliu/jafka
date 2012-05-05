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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.sohu.jafka.message.compress.CompressionFacade;
import com.sohu.jafka.message.compress.CompressionFactory;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class CompressionUtils {

    public static Message compress(Message[] messages, CompressionCodec compressionCodec) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CompressionCodec codec = compressionCodec;
        final CompressionFacade compressionFacade = CompressionFactory.create(//
                codec == CompressionCodec.DefaultCompressionCodec ? //
                CompressionCodec.GZIPCompressionCodec
                        : codec//
                , outputStream);
        ByteBuffer messageByteBuffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
        for (Message message : messages) {
            message.serializeTo(messageByteBuffer);
        }
        messageByteBuffer.rewind();
        try {
            compressionFacade.write(messageByteBuffer.array());
        } catch (IOException e) {
            throw new IllegalStateException("writting data failed", e);
        } finally {
            compressionFacade.close();
        }
        return new Message(outputStream.toByteArray(), compressionCodec);
    }

    public static ByteBufferMessageSet decompress(Message message) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        InputStream inputStream = new ByteBufferBackedInputStream(message.payload());
        //
        byte[] intermediateBuffer = new byte[1024];
        CompressionCodec codec = message.compressionCodec();
        final CompressionFacade compressionFacade = CompressionFactory.create(//
                codec == CompressionCodec.DefaultCompressionCodec ? //
                CompressionCodec.GZIPCompressionCodec
                        : codec//
                , inputStream);
        try {
            int dataRead = 0;
            while ((dataRead = compressionFacade.read(intermediateBuffer)) > 0) {
                outputStream.write(intermediateBuffer, 0, dataRead);
            }
        } catch (IOException e) {
            throw new IllegalStateException("decompression data failed", e);
        } finally {
            compressionFacade.close();
        }
        ByteBuffer outputBuffer = ByteBuffer.allocate(outputStream.size());
        outputBuffer.put(outputStream.toByteArray());
        outputBuffer.rewind();
        return new ByteBufferMessageSet(outputBuffer);
    }

}
