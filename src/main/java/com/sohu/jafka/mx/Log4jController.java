/**
 * 
 */
package com.sohu.jafka.mx;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * remote controller of log4j
 * 
 * @author adyliu(imxylz@gmail.com)
 * @since 1.3
 */
public class Log4jController implements Log4jControllerMBean, IMBeanName {

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getLoggers() {
        final List<String> loggers = new ArrayList<String>();
        loggers.add("root=" + existingLogger("root").getLevel().toString());
        Enumeration<Logger> en = LogManager.getCurrentLoggers();
        while (en.hasMoreElements()) {
            Logger logger = en.nextElement();
            if (logger != null) {
                loggers.add(String.format("%s=%s", logger.getName(), logger.getLevel() != null ? logger.getLevel().toString() : "null"));
            }
        }
        return loggers;
    }

    @Override
    public String getLogLevel(String loggerName) {
        Logger log = existingLogger(loggerName);
        if (log != null && log.getLevel() != null) {
            return log.getLevel().toString();
        }
        return "NO_LOGGER_OR_NO_LEVEL";
    }

    @Override
    public boolean setLogLevel(String loggerName, String level) {
        if (loggerName != null && loggerName.trim().length() > 0 && level != null && level.trim().length() > 0) {
            loggerName = loggerName.trim();
            level = level.trim().toUpperCase();
            Logger log = "root".equals(loggerName) ? LogManager.getRootLogger() : LogManager.getLogger(loggerName);
            log.setLevel(Level.toLevel(level));
            return true;
        }
        return false;
    }

    private Logger existingLogger(String loggerName) {
        if ("root".equals(loggerName)) {
            return LogManager.getRootLogger();
        }
        return LogManager.exists(loggerName);
    }

    @Override
    public String getMbeanName() {
        return "jafka:type=jafka.Log4jController";
    }
}
