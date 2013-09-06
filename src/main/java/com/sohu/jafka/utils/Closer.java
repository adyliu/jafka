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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.Selector;


/**
 * an useful tools to close some streams or file descriptions
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public final class Closer {

    private static final Logger closerLogger = LoggerFactory.getLogger(Closer.class);

    public static void close(java.io.Closeable closeable) throws IOException {
        close(closeable, closerLogger);
    }

    public static void close(java.io.Closeable closeable, Logger logger) throws IOException {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    public static void closeQuietly(Selector selector) {
        closeQuietly(selector, closerLogger);
    }

    public static void closeQuietly(java.io.Closeable closeable, Logger logger) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(java.nio.channels.Selector closeable, Logger logger) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(Socket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (IOException e) {
            closerLogger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(ServerSocket serverSocket){
        if(serverSocket == null)return;
        try {
            serverSocket.close();
        } catch (IOException e) {
            closerLogger.error(e.getMessage(),e);
        }
    }
    /**
     * Close a closeable object quietly(not throwing {@link IOException})
     * 
     * @param closeable A closeable object
     * @see java.io.Closeable
     */
    public static void closeQuietly(java.io.Closeable closeable) {
        closeQuietly(closeable, closerLogger);
    }
}
