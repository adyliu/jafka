package com.sohu.jafka.log;

/**
 * This strategy will be rolling file while it reaches the max file size.
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class FixedSizeRollingStrategy implements RollingStrategy {

    private final int maxFileSize;

    public FixedSizeRollingStrategy(int maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    @Override
    public boolean check(LogSegment lastSegment) {
        return lastSegment.getMessageSet().getSizeInBytes() > maxFileSize;
    }

}
