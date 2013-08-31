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

package com.sohu.jafka.server;

import com.sohu.jafka.log.LogManager;
import com.sohu.jafka.mx.Log4jController;
import com.sohu.jafka.mx.ServerInfo;
import com.sohu.jafka.mx.SocketServerStats;
import com.sohu.jafka.network.SocketServer;
import com.sohu.jafka.utils.Mx4jLoader;
import com.sohu.jafka.utils.Scheduler;
import com.sohu.jafka.utils.Utils;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The main server container
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Server implements Closeable {

    final String CLEAN_SHUTDOWN_FILE = ".jafka_cleanshutdown";

    final private Logger logger = Logger.getLogger(Server.class);

    final ServerConfig config;

    final Scheduler scheduler = new Scheduler(1, "jafka-logcleaner-", false);

    private LogManager logManager;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    SocketServer socketServer;

    private final File logDir;

    private final ServerInfo serverInfo = new ServerInfo();
    private final Log4jController log4jController = new Log4jController();

    //
    public Server(ServerConfig config) {
        this.config = config;
        logDir = new File(config.getLogDir());
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
    }

    public void startup() {
        try {
            logger.info("Starting Jafka server " + serverInfo.getVersion());
            Utils.registerMBean(serverInfo);
            Utils.registerMBean(log4jController);
            boolean needRecovery = true;
            File cleanShutDownFile = new File(new File(config.getLogDir()), CLEAN_SHUTDOWN_FILE);
            if (cleanShutDownFile.exists()) {
                needRecovery = false;
                cleanShutDownFile.delete();
            }
            this.logManager = new LogManager(config,//
                    scheduler,//
                    1000L * 60 * config.getLogCleanupIntervalMinutes(),//
                    1000L * 60 * 60 * config.getLogRetentionHours(),//
                    needRecovery);
            this.logManager.setRollingStategy(config.getRollingStrategy());
            logManager.load();

            RequestHandlers handlers = new RequestHandlers(logManager);
            socketServer = new SocketServer(handlers, config);
            Utils.registerMBean(socketServer.getStats());
            socketServer.startup();
            Mx4jLoader.maybeLoad();
            /**
             * Registers this broker in ZK. After this, consumers can connect to broker. So
             * this should happen after socket server start.
             */
            logManager.startup();
            logger.info("Server started.");
        } catch (Exception ex) {
            logger.fatal("Fatal error during startup.", ex);
            close();
        } finally {
            serverInfo.started();
        }
    }

    public void close() {
        boolean canShutdown = isShuttingDown.compareAndSet(false, true);
        if (!canShutdown) return;//CLOSED

        logger.info("Shutting down Jafka server...");
        try {
            scheduler.shutdown();
            if (socketServer != null) {
                socketServer.close();
                Utils.unregisterMBean(socketServer.getStats());
            }
            if (logManager != null) {
                logManager.close();
            }

            File cleanShutDownFile = new File(new File(config.getLogDir()), CLEAN_SHUTDOWN_FILE);
            cleanShutDownFile.createNewFile();
        } catch (Exception ex) {
            logger.fatal(ex.getMessage(), ex);
        }
        shutdownLatch.countDown();
        logger.info("shutdown Jafka server completed");
        Utils.unregisterMBean(log4jController);

    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }


    public LogManager getLogManager() {
        return logManager;
    }

    public SocketServerStats getStats() {
        return socketServer.getStats();
    }

}
