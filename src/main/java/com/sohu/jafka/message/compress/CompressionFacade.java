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

package com.sohu.jafka.message.compress;


import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.sohu.jafka.utils.Closer;

/**
 * message compression
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public abstract class CompressionFacade implements Closeable {

    protected final InputStream inputStream;

    protected final OutputStream outputStream;

    public CompressionFacade(InputStream inputStream, OutputStream outputStream) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
    }

    public void close() {
        Closer.closeQuietly(inputStream);
        Closer.closeQuietly(outputStream);
    }

    public int read(byte[] b) throws IOException {
        return inputStream.read(b);
    }

    public void write(byte[] b) throws IOException {
        outputStream.write(b);
    }
}
