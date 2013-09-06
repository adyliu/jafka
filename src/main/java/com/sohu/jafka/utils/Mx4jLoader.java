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

/**
 * If mx4j-tools is in the classpath call maybeLoad to load the HTTP interface of mx4j.
 * 
 * The default port is 8082. To override that provide e.g. -Dmx4jport=8083 The default listen
 * address is 0.0.0.0. To override that provide -Dmx4jaddress=127.0.0.1 This feature must be
 * enabled with -Dmx4jenable=true
 * 
 * <p>
 * see https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/utils/
 * Mx4jTool.java
 * </p>
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Mx4jLoader {

    private static final Logger logger = LoggerFactory.getLogger(Mx4jLoader.class);

    private static Object httpAdaptor;

    private static Class<?> httpAdaptorClass;

    /**
     * Starts a JMX over http interface if and mx4j-tools.jar is in the classpath.
     * 
     * @return true if successfully loaded.
     */
    public synchronized static boolean maybeLoad() {
        try {
            if (!Utils.getBoolean(System.getProperties(), "jafka_mx4jenable", false)) {
                return false;
            }
            if (httpAdaptor != null) {
                logger.warn("mx4j has started");
                return true;
            }
            logger.debug("Will try to load mx4j now, if it's in the classpath");
            //MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            //ObjectName processorName = new ObjectName("Server:name=XSLTProcessor");

            httpAdaptorClass = Class.forName("mx4j.tools.adaptor.http.HttpAdaptor");
            httpAdaptor = httpAdaptorClass.newInstance();
            httpAdaptorClass.getMethod("setHost", String.class).invoke(httpAdaptor, getAddress());
            httpAdaptorClass.getMethod("setPort", Integer.TYPE).invoke(httpAdaptor, getPort());

            //ObjectName httpName = new ObjectName("system:name=http");
            //mbs.registerMBean(httpAdaptor, httpName);
            Utils.registerMBean(httpAdaptor, "system:name=http");

            Class<?> xsltProcessorClass = Class.forName("mx4j.tools.adaptor.http.XSLTProcessor");
            Object xsltProcessor = xsltProcessorClass.newInstance();
            httpAdaptorClass.getMethod("setProcessor", Class.forName("mx4j.tools.adaptor.http.ProcessorMBean")).invoke(
                    httpAdaptor, xsltProcessor);
            //mbs.registerMBean(xsltProcessor, processorName);
            Utils.registerMBean(xsltProcessor, "Server:name=XSLTProcessor");
            httpAdaptorClass.getMethod("start").invoke(httpAdaptor);
            logger.info("mx4j successfuly loaded");
            return true;
        } catch (ClassNotFoundException e) {
            logger.info("Will not load MX4J, mx4j-tools.jar is not in the classpath");
        } catch (Exception e) {
            logger.warn("Could not start register mbean in JMX", e);
        }
        return false;
    }

    public static synchronized void close() {
        if (httpAdaptor == null) {
            return;
        }
        try {
            httpAdaptorClass.getMethod("stop").invoke(httpAdaptor);
        } catch (Exception e) {
            logger.warn("close mx4j failed. " + e.getMessage());
        }
    }

    private static String getAddress() {
        return System.getProperty("mx4jaddress", "0.0.0.0");
    }

    private static int getPort() {
        return Utils.getInt(System.getProperties(), "mx4jport", 8082);
    }
}
