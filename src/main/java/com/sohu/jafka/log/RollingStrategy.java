package com.sohu.jafka.log;

import java.io.Closeable;


/**
 * log rolling strategy
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public interface RollingStrategy extends Closeable{

    /**
     * the the last LogSegment whether needing rolling over
     * 
     * @param lastSegment the last segment
     * @return true meaning rolling over and false doing nothing
     */
    boolean check(LogSegment lastSegment);
}
