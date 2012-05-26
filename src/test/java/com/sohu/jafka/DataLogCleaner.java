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

import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class DataLogCleaner {

    public static final String defaultBuildPath = "./build";

    public static final String defaultDataLogPath = defaultBuildPath + "/data";

    public static final File defaultBuildDir = Utils.getCanonicalFile(new File(defaultBuildPath));

    public static final File defaultDataLogDir = Utils.getCanonicalFile(new File(defaultDataLogPath));

    public static void cleanDataLogDir() {
        cleanDataLogDir(defaultBuildDir);
    }

    public static void cleanDataLogDir(File dir) {
        if (!dir.exists()) return;
        File[] subs = dir.listFiles();
        if (subs != null) {
            for (File f : dir.listFiles()) {
                if (f.isFile()) {
                    if(!f.delete()) {
                        throw new IllegalStateException("delete file failed: "+f);
                    }
                } else {
                    cleanDataLogDir(f);
                }
            }
        }
        if(!dir.delete()) {
            throw new IllegalStateException("delete directory failed: "+dir);
        }
    }
}
