package com.sohu.jafka;

import java.util.Properties;

import org.junit.Test;

import com.sohu.jafka.utils.Closer;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class JafkaTest {

    /**
     * Test method for
     * {@link com.sohu.jafka.Jafka#start(java.util.Properties, java.util.Properties, java.util.Properties)}
     * .
     * 
     * @throws InterruptedException
     */
    @Test
    public void testStartPropertiesPropertiesProperties() {
        DataLogCleaner.cleanDataLogDir();
        Jafka jafka = new Jafka();
        Properties mainProperties = new Properties();
        mainProperties.setProperty("brokerid", "0");
        mainProperties.setProperty("log.dir", DataLogCleaner.defaultDataLogPath);
        jafka.start(mainProperties, null, null);
        Closer.closeQuietly(jafka);
        jafka.awaitShutdown();
    }

}
