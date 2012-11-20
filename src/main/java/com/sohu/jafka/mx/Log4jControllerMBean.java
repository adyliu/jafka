/**
 * 
 */
package com.sohu.jafka.mx;

import java.util.List;

/**
 * remote controller of log4j
 * 
 * @author adyliu(imxylz@gmail.com)
 * @since 2012-11-20
 * @since 1.3
 */
public interface Log4jControllerMBean {
    /**
     * get all loggers seperated by '='.<br/>
     * for example: <code>root=INFO</code>
     * @return all loggers
     */
    List<String> getLoggers();

    String getLogLevel(String loggerName);

    boolean setLogLevel(String loggerName, String level);
}
