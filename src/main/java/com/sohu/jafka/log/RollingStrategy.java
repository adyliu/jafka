package com.sohu.jafka.log;


/**
 * log rolling strategy
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public interface RollingStrategy {

    /**
     * the the last LogSegment whether needing rolling over
     * 
     * @param lastSegment the last segment
     * @return true meaning rolling over and false doing nothing
     */
    boolean check(LogSegment lastSegment);
}
