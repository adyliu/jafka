package com.sohu.jafka.log;

import static java.lang.String.format;

import java.io.IOException;

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

    @Override
    public String toString() {
        return format("FixedSizeRollingStrategy [maxFileSize=%d bytes(%dMB)", maxFileSize, maxFileSize / (1024 * 1024));
    }

    @Override
    public void close() throws IOException {
    }
}
