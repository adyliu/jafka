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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public abstract class BaseJafkaServer {

    static {
        System.setProperty("jafka_mx4jenable", "true");
    }

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public Jafka createJafka() {
        Properties mainProperties = new Properties();
        return createJafka(mainProperties);
    }

    public Jafka createJafka(Properties mainProperties) {
        Jafka jafka = new Jafka();
        if (!mainProperties.containsKey("brokerid")) {
            mainProperties.setProperty("brokerid", "0");
        }
        if (!mainProperties.containsKey("log.dir")) {
            mainProperties.setProperty("log.dir", DataLogCleaner.defaultDataLogPath);
        }
        if(!mainProperties.containsKey("port")){
            mainProperties.setProperty("port",""+PortUtils.checkAvailablePort(9092));
        }
        mainProperties.setProperty("num.threads",String.valueOf(Math.min(2,Runtime.getRuntime().availableProcessors())));
        DataLogCleaner.cleanDataLogDir(new File(mainProperties.getProperty("log.dir")));
        jafka.start(mainProperties, null, null);
        return jafka;
    }

    public void flush(Jafka jafka) {
        jafka.flush();
    }

    public void close(Jafka jafka) {
        if (jafka != null) {
            jafka.close();
            jafka.awaitShutdown();
        }
    }
}
