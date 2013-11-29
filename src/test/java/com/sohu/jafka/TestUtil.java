package com.sohu.jafka;

import org.junit.Ignore;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Ignore
public class TestUtil {

    static {
        System.setProperty("zookeeper.preAllocSize", "1024");//1M data log
    }
    /**
     * This waits until the provided {@link Callable} returns an object that is equals to the given expected value or
     * the timeout has been reached. In both cases this method will return the return value of the latest
     * {@link Callable} execution.
     * 
     * @param expectedValue
     *            The expected value of the callable.
     * @param callable
     *            The callable.
     * @param <T>
     *            The return type of the callable.
     * @param timeUnit
     *            The timeout timeunit.
     * @param timeout
     *            The timeout.
     * @return the return value of the latest {@link Callable} execution.
     * @throws Exception
     * @throws InterruptedException
     */
    public static <T> T waitUntil(T expectedValue, Callable<T> callable, TimeUnit timeUnit, long timeout) throws Exception {
        long startTime = System.currentTimeMillis();
        do {
            T actual = callable.call();
            if (expectedValue.equals(actual)) {
                return actual;
            }
            if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
                return actual;
            }
            Thread.sleep(50);
        } while (true);
    }

}