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

package com.sohu.jafka;

import java.io.File;
import java.io.IOException;

import com.github.zkclient.ZkServer;

/**
 * @author adyliu (imxylz@gmail.com)
 */
public class ZkServerTestUtil {

    static {
        System.setProperty("zookeeper.preAllocSize", "1024");//1M data log
    }


    public static ZkServer startZkServer(int port) throws IOException {
        final String dataPath=DataLogCleaner.defaultBuildPath+"/zk/default/data";
        final String logPath=DataLogCleaner.defaultBuildPath+"/zk/default/log";
        File dataDir = new File(dataPath);
        File logDir = new File(logPath);
        DataLogCleaner.cleanDataLogDir(dataDir);
        DataLogCleaner.cleanDataLogDir(logDir);
        dataDir.mkdirs();
        logDir.mkdirs();
        ZkServer zkServer = new ZkServer(dataPath, logPath, port,
                ZkServer.DEFAULT_TICK_TIME, 100);
        zkServer.start();
        return zkServer;
    }

    public static void closeZkServer(ZkServer server) {
        if (server != null) {
            server.shutdown();
        }
    }
}
