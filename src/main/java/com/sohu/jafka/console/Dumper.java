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

import static java.lang.String.format;

import java.io.File;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.sohu.jafka.message.FileMessageSet;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.utils.Utils;

/**
 * String message dumper
 * @author adyliu (imxylz@gmail.com)
 * @since 1.1
 */
public class Dumper {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("no-message", "do not decode message(utf-8 strings)");
        parser.accepts("no-offset", "do not print message offset");
        parser.accepts("no-size", "do not print message size");
        ArgumentAcceptingOptionSpec<Integer> countOpt = parser.accepts("c", "max count mesages.")//
                .withRequiredArg().describedAs("count")//
                .ofType(Integer.class).defaultsTo(-1);
        ArgumentAcceptingOptionSpec<String> fileOpt = parser.accepts("file", "decode file list")//
                .withRequiredArg().ofType(String.class).describedAs("filepath");
        OptionSet options = parser.parse(args);
        if (!options.has(fileOpt)) {
            System.err.println("Usage: [options] --file <file>...");
            parser.printHelpOn(System.err);
            System.exit(1);
        }
        final boolean decode = !options.has("no-message");
        final boolean withOffset = !options.has("no-offset");
        final boolean withSize = !options.has("no-size");
        int count = countOpt.value(options);
        count = count <= 0 ? Integer.MAX_VALUE : count;
        int index = 0;
        final String logformat = "%s|%s|%s";
        for (String filepath : fileOpt.values(options)) {
            if (index >= count) break;
            File file = new File(filepath);
            String filename = file.getName();
            final long startOffset = Long.parseLong(filename.substring(0, filename.lastIndexOf('.')));
            FileMessageSet messageSet = new FileMessageSet(file, false);
            long offset = 0L;
            long totalSize = 0L;
            try {
                int messageCount = 0;
                for (MessageAndOffset mao : messageSet) {
                    final Message msg = mao.message;
                    if (index >= count) {
                        break;
                    }
                    if (decode || withOffset || withSize) {
                        System.out.println(format(logformat,//
                                withOffset ? "" + (startOffset + offset) : "",//
                                withSize ? "" + msg.payloadSize() : "",//
                                decode ? Utils.toString(msg.payload(), "UTF-8") : ""));
                    }
                    offset = mao.offset;
                    totalSize += msg.payloadSize();
                    messageCount++;
                    index++;
                }
                System.out.println("-----------------------------------------------------------------------------");
                System.out.println(filepath);
                System.out.println("total message count: " + messageCount);
                System.out.println("total message size: " + totalSize);
                System.out.println("=============================================");
            } finally {
                messageSet.close();
            }
        }
    }

}
