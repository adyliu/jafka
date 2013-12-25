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

package com.sohu.jafka.log;

import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.api.PartitionChooser;
import com.sohu.jafka.common.InvalidPartitionException;
import com.sohu.jafka.server.ServerConfig;
import com.sohu.jafka.server.ServerRegister;
import com.sohu.jafka.server.TopicTask;
import com.sohu.jafka.server.TopicTask.TaskType;
import com.sohu.jafka.utils.Closer;
import com.sohu.jafka.utils.IteratorTemplate;
import com.sohu.jafka.utils.KV;
import com.sohu.jafka.utils.Pool;
import com.sohu.jafka.utils.Scheduler;
import com.sohu.jafka.utils.TopicNameValidator;
import com.sohu.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;

/**
 * The log management system is responsible for log creation, retrieval, and cleaning.
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class LogManager implements PartitionChooser, Closeable {

    final ServerConfig config;

    private final Scheduler scheduler;

    final long logCleanupIntervalMs;

    final long logCleanupDefaultAgeMs;

    final boolean needRecovery;

    private final Logger logger = LoggerFactory.getLogger(LogManager.class);

    ///////////////////////////////////////////////////////////////////////
    final int numPartitions;

    final File logDir;

    final int flushInterval;

    private final Object logCreationLock = new Object();

    final Random random = new Random();

    final CountDownLatch startupLatch;

    //
    private final Pool<String, Pool<Integer, Log>> logs = new Pool<String, Pool<Integer, Log>>();

    private final Scheduler logFlusherScheduler = new Scheduler(1, "jafka-logflusher-", false);

    //
    private final LinkedBlockingQueue<TopicTask> topicRegisterTasks = new LinkedBlockingQueue<TopicTask>();

    private volatile boolean stopTopicRegisterTasks = false;

    final Map<String, Integer> logFlushIntervalMap;

    final Map<String, Long> logRetentionMSMap;

    final int logRetentionSize;

    /////////////////////////////////////////////////////////////////////////
    private ServerRegister serverRegister;

    private final Map<String, Integer> topicPartitionsMap;

    private RollingStrategy rollingStategy;

    private final int maxMessageSize;

    public LogManager(ServerConfig config, //
                      Scheduler scheduler, //
                      long logCleanupIntervalMs, //
                      long logCleanupDefaultAgeMs, //
                      boolean needRecovery) {
        super();
        this.config = config;
        this.maxMessageSize = config.getMaxMessageSize();
        this.scheduler = scheduler;
        //        this.time = time;
        this.logCleanupIntervalMs = logCleanupIntervalMs;
        this.logCleanupDefaultAgeMs = logCleanupDefaultAgeMs;
        this.needRecovery = needRecovery;
        //
        this.logDir = Utils.getCanonicalFile(new File(config.getLogDir()));
        this.numPartitions = config.getNumPartitions();
        this.flushInterval = config.getFlushInterval();
        this.topicPartitionsMap = config.getTopicPartitionsMap();
        this.startupLatch = config.getEnableZookeeper() ? new CountDownLatch(1) : null;
        this.logFlushIntervalMap = config.getFlushIntervalMap();
        this.logRetentionSize = config.getLogRetentionSize();
        this.logRetentionMSMap = getLogRetentionMSMap(config.getLogRetentionHoursMap());
        //
    }

    public void setRollingStategy(RollingStrategy rollingStategy) {
        this.rollingStategy = rollingStategy;
    }

    public void load() throws IOException {
        if (this.rollingStategy == null) {
            this.rollingStategy = new FixedSizeRollingStrategy(config.getLogFileSize());
        }
        if (!logDir.exists()) {
            logger.info("No log directory found, creating '" + logDir.getAbsolutePath() + "'");
            logDir.mkdirs();
        }
        if (!logDir.isDirectory() || !logDir.canRead()) {
            throw new IllegalArgumentException(logDir.getAbsolutePath() + " is not a readable log directory.");
        }
        File[] subDirs = logDir.listFiles();
        if (subDirs != null) {
            for (File dir : subDirs) {
                if (!dir.isDirectory()) {
                    logger.warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "'--should it be there?");
                } else {
                    logger.info("Loading log from " + dir.getAbsolutePath());
                    final String topicNameAndPartition = dir.getName();
                    if (-1 == topicNameAndPartition.indexOf('-')) {
                        throw new IllegalArgumentException("error topic directory: " + dir.getAbsolutePath());
                    }
                    final KV<String, Integer> topicPartion = Utils.getTopicPartition(topicNameAndPartition);
                    final String topic = topicPartion.k;
                    final int partition = topicPartion.v;
                    Log log = new Log(dir, partition, this.rollingStategy, flushInterval, needRecovery, maxMessageSize);

                    logs.putIfNotExists(topic, new Pool<Integer, Log>());
                    Pool<Integer, Log> parts = logs.get(topic);

                    parts.put(partition, log);
                    int configPartition = getPartition(topic);
                    if (configPartition <= partition) {
                        topicPartitionsMap.put(topic, partition + 1);
                    }
                }
            }
        }

        /* Schedule the cleanup task to delete old logs */
        if (this.scheduler != null) {
            logger.debug("starting log cleaner every " + logCleanupIntervalMs + " ms");
            this.scheduler.scheduleWithRate(new Runnable() {

                public void run() {
                    try {
                        cleanupLogs();
                    } catch (IOException e) {
                        logger.error("cleanup log failed.", e);
                    }
                }

            }, 60 * 1000, logCleanupIntervalMs);
        }
        //
        if (config.getEnableZookeeper()) {
            this.serverRegister = new ServerRegister(config, this);
            serverRegister.startup();
            TopicRegisterTask task = new TopicRegisterTask();
            task.setName("jafka.topicregister");
            task.setDaemon(true);
            task.start();
        }
    }

    private void registeredTaskLooply() {
        while (!stopTopicRegisterTasks) {
            try {
                TopicTask task = topicRegisterTasks.take();
                if (task.type == TaskType.SHUTDOWN) break;
                serverRegister.processTask(task);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        logger.debug("stop topic register task");
    }

    class TopicRegisterTask extends Thread {

        @Override
        public void run() {
            registeredTaskLooply();
        }
    }

    private Map<String, Long> getLogRetentionMSMap(Map<String, Integer> logRetentionHourMap) {
        Map<String, Long> ret = new HashMap<String, Long>();
        for (Map.Entry<String, Integer> e : logRetentionHourMap.entrySet()) {
            ret.put(e.getKey(), e.getValue() * 60 * 60 * 1000L);
        }
        return ret;
    }

    public void close() {
        logFlusherScheduler.shutdown();
        Iterator<Log> iter = getLogIterator();
        while (iter.hasNext()) {
            Closer.closeQuietly(iter.next(), logger);
        }
        if (config.getEnableZookeeper()) {
            stopTopicRegisterTasks = true;
            //wake up again and again
            topicRegisterTasks.add(new TopicTask(TaskType.SHUTDOWN, null));
            topicRegisterTasks.add(new TopicTask(TaskType.SHUTDOWN, null));
            Closer.closeQuietly(serverRegister);
        }
    }

    /**
     * Runs through the log removing segments older than a certain age
     *
     * @throws IOException
     */
    private void cleanupLogs() throws IOException {
        logger.trace("Beginning log cleanup...");
        int total = 0;
        Iterator<Log> iter = getLogIterator();
        long startMs = System.currentTimeMillis();
        while (iter.hasNext()) {
            Log log = iter.next();
            total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log);
        }
        if (total > 0) {
            logger.warn("Log cleanup completed. " + total + " files deleted in " + (System.currentTimeMillis() - startMs) / 1000 + " seconds");
        } else {
            logger.trace("Log cleanup completed. " + total + " files deleted in " + (System.currentTimeMillis() - startMs) / 1000 + " seconds");
        }
    }

    /**
     * Runs through the log removing segments until the size of the log is at least
     * logRetentionSize bytes in size
     *
     * @throws IOException
     */
    private int cleanupSegmentsToMaintainSize(final Log log) throws IOException {
        if (logRetentionSize < 0 || log.size() < logRetentionSize) return 0;

        List<LogSegment> toBeDeleted = log.markDeletedWhile(new LogSegmentFilter() {

            long diff = log.size() - logRetentionSize;

            public boolean filter(LogSegment segment) {
                diff -= segment.size();
                return diff >= 0;
            }
        });
        return deleteSegments(log, toBeDeleted);
    }

    private int cleanupExpiredSegments(Log log) throws IOException {
        final long startMs = System.currentTimeMillis();
        String topic = Utils.getTopicPartition(log.dir.getName()).k;
        Long logCleanupThresholdMS = logRetentionMSMap.get(topic);
        if (logCleanupThresholdMS == null) {
            logCleanupThresholdMS = this.logCleanupDefaultAgeMs;
        }
        final long expiredThrshold = logCleanupThresholdMS.longValue();
        List<LogSegment> toBeDeleted = log.markDeletedWhile(new LogSegmentFilter() {

            public boolean filter(LogSegment segment) {
                //check file which has not been modified in expiredThrshold millionseconds
                return startMs - segment.getFile().lastModified() > expiredThrshold;
            }
        });
        return deleteSegments(log, toBeDeleted);
    }

    /**
     * Attemps to delete all provided segments from a log and returns how many it was able to
     */
    private int deleteSegments(Log log, List<LogSegment> segments) {
        int total = 0;
        for (LogSegment segment : segments) {
            boolean deleted = false;
            try {
                try {
                    segment.getMessageSet().close();
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
                if (!segment.getFile().delete()) {
                    deleted = true;
                } else {
                    total += 1;
                }
            } finally {
                logger.warn(String.format("DELETE_LOG[%s] %s => %s", log.name, segment.getFile().getAbsolutePath(),
                        deleted));
            }
        }
        return total;
    }

    /**
     * Register this broker in ZK for the first time.
     */
    public void startup() {
        if (config.getEnableZookeeper()) {
            serverRegister.registerBrokerInZk();
            for (String topic : getAllTopics()) {
                serverRegister.processTask(new TopicTask(TaskType.CREATE, topic));
            }
            startupLatch.countDown();
        }
        logger.debug("Starting log flusher every {} ms with the following overrides {}", config.getFlushSchedulerThreadRate(), logFlushIntervalMap);
        logFlusherScheduler.scheduleWithRate(new Runnable() {

            public void run() {
                flushAllLogs(false);
            }
        }, config.getFlushSchedulerThreadRate(), config.getFlushSchedulerThreadRate());
    }

    /**
     * flush all messages to disk
     *
     * @param force flush anyway(ignore flush interval)
     */
    public void flushAllLogs(final boolean force) {
        Iterator<Log> iter = getLogIterator();
        while (iter.hasNext()) {
            Log log = iter.next();
            try {
                boolean needFlush = force;
                if (!needFlush) {
                    long timeSinceLastFlush = System.currentTimeMillis() - log.getLastFlushedTime();
                    Integer logFlushInterval = logFlushIntervalMap.get(log.getTopicName());
                    if (logFlushInterval == null) {
                        logFlushInterval = config.getDefaultFlushIntervalMs();
                    }
                    final String flushLogFormat = "[%s] flush interval %d, last flushed %d, need flush? %s";
                    needFlush = timeSinceLastFlush >= logFlushInterval.intValue();
                    logger.trace(String.format(flushLogFormat, log.getTopicName(), logFlushInterval,
                            log.getLastFlushedTime(), needFlush));
                }
                if (needFlush) {
                    log.flush();
                }
            } catch (IOException ioe) {
                logger.error("Error flushing topic " + log.getTopicName(), ioe);
                logger.error("Halting due to unrecoverable I/O error while flushing logs: " + ioe.getMessage(), ioe);
                Runtime.getRuntime().halt(1);
            } catch (Exception e) {
                logger.error("Error flushing topic " + log.getTopicName(), e);
            }
        }
    }

    private Collection<String> getAllTopics() {
        return logs.keySet();
    }

    private Iterator<Log> getLogIterator() {
        return new IteratorTemplate<Log>() {

            final Iterator<Pool<Integer, Log>> iterator = logs.values().iterator();

            Iterator<Log> logIter;

            @Override
            protected Log makeNext() {
                while (true) {
                    if (logIter != null && logIter.hasNext()) {
                        return logIter.next();
                    }
                    if (!iterator.hasNext()) {
                        return allDone();
                    }
                    logIter = iterator.next().values().iterator();
                }
            }
        };
    }

    private void awaitStartup() {
        if (config.getEnableZookeeper()) {
            try {
                startupLatch.await();
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    private Pool<Integer, Log> getLogPool(String topic, int partition) {
        awaitStartup();
        if (topic.length() <= 0) {
            throw new IllegalArgumentException("topic name can't be empty");
        }
        //
        Integer definePartition = this.topicPartitionsMap.get(topic);
        if (definePartition == null) {
            definePartition = numPartitions;
        }
        if (partition < 0 || partition >= definePartition.intValue()) {
            String msg = "Wrong partition [%d] for topic [%s], valid partitions [0,%d)";
            msg = format(msg, partition, topic, definePartition.intValue() - 1);
            logger.warn(msg);
            throw new InvalidPartitionException(msg);
        }
        return logs.get(topic);
    }

    /**
     * Get the log if exists or return null
     *
     * @param topic     topic name
     * @param partition partition index
     * @return a log for the topic or null if not exist
     */
    public ILog getLog(String topic, int partition) {
        TopicNameValidator.validate(topic);
        Pool<Integer, Log> p = getLogPool(topic, partition);
        return p == null ? null : p.get(partition);
    }

    /**
     * Create the log if it does not exist or return back exist log
     *
     * @param topic     the topic name
     * @param partition the partition id
     * @return read or create a log
     * @throws IOException any IOException
     */
    public ILog getOrCreateLog(String topic, int partition) throws IOException {
        final int configPartitionNumber = getPartition(topic);
        if (partition >= configPartitionNumber) {
            throw new IOException("partition is bigger than the number of configuration: " + configPartitionNumber);
        }
        boolean hasNewTopic = false;
        Pool<Integer, Log> parts = getLogPool(topic, partition);
        if (parts == null) {
            Pool<Integer, Log> found = logs.putIfNotExists(topic, new Pool<Integer, Log>());
            if (found == null) {
                hasNewTopic = true;
            }
            parts = logs.get(topic);
        }
        //
        Log log = parts.get(partition);
        if (log == null) {
            log = createLog(topic, partition);
            Log found = parts.putIfNotExists(partition, log);
            if (found != null) {
                Closer.closeQuietly(log, logger);
                log = found;
            } else {
                logger.info(format("Created log for [%s-%d], now create other logs if necessary", topic, partition));
                final int configPartitions = getPartition(topic);
                for (int i = 0; i < configPartitions; i++) {
                    getOrCreateLog(topic, i);
                }
            }
        }
        if (hasNewTopic && config.getEnableZookeeper()) {
            topicRegisterTasks.add(new TopicTask(TaskType.CREATE, topic));
        }
        return log;
    }

    /**
     * create logs with given partition number
     *
     * @param topic        the topic name
     * @param partitions   partition number
     * @param forceEnlarge enlarge the partition number of log if smaller than runtime
     * @return the partition number of the log after enlarging
     */
    public int createLogs(String topic, final int partitions, final boolean forceEnlarge) {
        TopicNameValidator.validate(topic);
        synchronized (logCreationLock) {
            final int configPartitions = getPartition(topic);
            if (configPartitions >= partitions || !forceEnlarge) {
                return configPartitions;
            }
            topicPartitionsMap.put(topic, partitions);
            if (config.getEnableZookeeper()) {
                if (getLogPool(topic, 0) != null) {//created already
                    topicRegisterTasks.add(new TopicTask(TaskType.ENLARGE, topic));
                } else {
                    topicRegisterTasks.add(new TopicTask(TaskType.CREATE, topic));
                }
            }
            return partitions;
        }
    }

    /**
     * delete topic who is never used
     * <p>
     * This will delete all log files and remove node data from zookeeper
     * </p>
     *
     * @param topic topic name
     * @return number of deleted partitions or -1 if authentication failed
     */
    public int deleteLogs(String topic, String password) {
        if (!config.getAuthentication().auth(password)) {
            return -1;
        }
        int value = 0;
        synchronized (logCreationLock) {
            Pool<Integer, Log> parts = logs.remove(topic);
            if (parts != null) {
                List<Log> deleteLogs = new ArrayList<Log>(parts.values());
                for (Log log : deleteLogs) {
                    log.delete();
                    value++;
                }
            }
            if (config.getEnableZookeeper()) {
                topicRegisterTasks.add(new TopicTask(TaskType.DELETE, topic));
            }
        }
        return value;
    }

    private Log createLog(String topic, int partition) throws IOException {
        synchronized (logCreationLock) {
            File d = new File(logDir, topic + "-" + partition);
            d.mkdirs();
            return new Log(d, partition, this.rollingStategy, flushInterval, false, maxMessageSize);
        }
    }

    private int getPartition(String topic) {
        Integer p = topicPartitionsMap.get(topic);
        return p != null ? p.intValue() : this.numPartitions;
    }

    /**
     * Pick a random partition from the given topic
     */
    public int choosePartition(String topic) {
        return random.nextInt(getPartition(topic));
    }

    /**
     * read offsets before given time
     *
     * @param offsetRequest the offset request
     * @return offsets before given time
     */
    public List<Long> getOffsets(OffsetRequest offsetRequest) {
        ILog log = getLog(offsetRequest.topic, offsetRequest.partition);
        if (log != null) {
            return log.getOffsetsBefore(offsetRequest);
        }
        return ILog.EMPTY_OFFSETS;
    }

    public Map<String, Integer> getTopicPartitionsMap() {
        return topicPartitionsMap;
    }

}
