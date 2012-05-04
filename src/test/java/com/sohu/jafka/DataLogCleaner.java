package com.sohu.jafka;

import java.io.File;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class DataLogCleaner {

    public static final String defaultDataLogPath = "./data";

    public static final File defaultDataLogDir = new File(defaultDataLogPath);

    public static void cleanDataLogDir() {
        cleanDataLogDir(defaultDataLogDir);
    }

    public static void cleanDataLogDir(File dir) {
        for (File f : dir.listFiles()) {
            if (f.isFile()) {
                f.delete();
            } else {
                cleanDataLogDir(f);
            }
        }
        dir.delete();
    }
}
