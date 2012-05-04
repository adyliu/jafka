package com.sohu.jafka;

import java.util.Properties;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public abstract class BaseJafkaServer {

    public Jafka createJafka() {
        Properties mainProperties = new Properties();
        return createJafka(mainProperties);
    }
    public Jafka createJafka(Properties mainProperties) {
        DataLogCleaner.cleanDataLogDir();
        Jafka jafka = new Jafka();
       
        mainProperties.setProperty("brokerid", "0");
        mainProperties.setProperty("log.dir", DataLogCleaner.defaultDataLogPath);
        jafka.start(mainProperties, null, null);
        return jafka;
    }

    public void close(Jafka jafka) {
        if (jafka != null) {
            jafka.close();
            jafka.awaitShutdown();
        }
    }
}
