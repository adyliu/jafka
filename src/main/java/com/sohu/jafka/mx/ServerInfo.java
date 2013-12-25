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

package com.sohu.jafka.mx;

import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.String.format;

/**
 * Some thing about the Jafka server
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.1
 */
public class ServerInfo implements ServerInfoMBean, IMBeanName {

    private final String startupTime;

    private String startedTime;

    private final long startupDateTime;

    public ServerInfo() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        startupTime = format.format(new Date());
        startupDateTime = System.currentTimeMillis();
    }

    @Override
    public String getVersion() {
        String version = null;
        try {
            version = ServerInfo.class.getPackage().getSpecificationVersion();
        } catch (Exception e) {
        }
        return version != null ? version : "";
    }

    @Override
    public String getStartupTime() {
        return startupTime;
    }

    public void started() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        startedTime = format.format(new Date());
    }

    @Override
    public String getRunningTime() {
        long time = System.currentTimeMillis() - startupDateTime;
        final long days = time / (1000L * 60 * 60 * 24L);
        time = time % (1000L * 60 * 60 * 24L);
        final long hours = time / (1000L * 60 * 60);
        time = time % (1000L * 60 * 60);
        final long minutes = time / (1000L * 60);
        return format("%d Days %d Hours %d Minutes", days, hours, minutes);
    }

    @Override
    public String getStartedTime() {
        return startedTime != null ? startedTime : "--";
    }

    @Override
    public String getMbeanName() {
        return "jafka:type=jafka.ServerInfo";
    }
}
