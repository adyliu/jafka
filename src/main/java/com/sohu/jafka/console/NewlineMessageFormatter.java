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

package com.sohu.jafka.console;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Properties;

import com.sohu.jafka.message.Message;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class NewlineMessageFormatter implements MessageFormatter {

    public void writeTo(Message message, PrintStream output) {
        ByteBuffer buffer = message.payload();
        output.write(buffer.array(), buffer.arrayOffset(), buffer.limit());
        output.write('\n');
    }

    public void init(Properties props) {
    }

    public void close() {
    }

}
