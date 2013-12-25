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

import com.sohu.jafka.utils.Utils;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Rolling file every day
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.1
 */
//FIXME: NOT WORK AT STARTUP
public class DailyRollingStrategy implements RollingStrategy, Runnable {

    private boolean firstCheck = true;

    private final long ONE_HOUR = 1000L * 60 * 60;

    private final long ONE_DAY = ONE_HOUR * 24;

    private volatile boolean running = true;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition waitCondition = lock.newCondition();

    //
    private final AtomicBoolean needRolling = new AtomicBoolean(false);

    private volatile long lastRollingTime = 0;

    @Override
    public boolean check(LogSegment lastSegment) {
        if (firstCheck) {
            checkFile(lastSegment);
        }
        if (needRolling.get()) {
            lastRollingTime = today().getTimeInMillis();
            needRolling.set(false);
            waitCondition.notifyAll();
            return true;
        }
        return false;
    }

    private void checkFile(LogSegment lastSegment) {
        firstCheck = false;
        Calendar today = today();
        if (lastSegment.getFile().lastModified() < today.getTimeInMillis()) {
            needRolling.set(true);
        }
        lastRollingTime = today.getTimeInMillis();
        Utils.newThread("jafka-daily-rolling", this, true).start();
    }

    private Calendar today() {
        Calendar today = Calendar.getInstance();
        today.set(Calendar.HOUR_OF_DAY, 0);
        today.set(Calendar.MINUTE, 0);
        today.set(Calendar.SECOND, 0);
        today.set(Calendar.MILLISECOND, 0);
        return today;
    }

    @Override
    public void run() {
        while (running) {
            Calendar today = today();
            today.add(Calendar.DAY_OF_MONTH, 1);
            lock.lock();
            try {
                waitCondition.awaitUntil(new Date(today.getTimeInMillis()));
                if (System.currentTimeMillis() - lastRollingTime >= ONE_DAY) {
                    needRolling.compareAndSet(false, true);
                }
            } catch (InterruptedException e) {
                running = false;
                break;
            } finally {
                lock.unlock();
            }

        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        waitCondition.notifyAll();
    }

}
