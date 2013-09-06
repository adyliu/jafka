/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sohu.jafka.message;

import com.sohu.jafka.mx.LogFlushStats;
import com.sohu.jafka.utils.IteratorTemplate;
import com.sohu.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An on-disk message set. The set can be opened either mutably or immutably. Mutation attempts
 * will fail on an immutable message set. An optional limit and offset can be applied to the
 * message set which will control the offset into the file and the effective length into the
 * file from which messages will be read
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class FileMessageSet extends MessageSet {

    private final Logger logger = LoggerFactory.getLogger(FileMessageSet.class);

    private final FileChannel channel;

    private final long offset;

    private final boolean mutable;

    private final AtomicBoolean needRecover;

    /////////////////////////////////////////////////////////////////////////
    private final AtomicLong setSize = new AtomicLong();

    private final AtomicLong setHighWaterMark = new AtomicLong();

    public FileMessageSet(FileChannel channel, long offset, long limit, //
                          boolean mutable, AtomicBoolean needRecover) throws IOException {
        super();
        this.channel = channel;
        this.offset = offset;
        this.mutable = mutable;
        this.needRecover = needRecover;
        if (mutable) {
            if (limit < Long.MAX_VALUE || offset > 0) throw new IllegalArgumentException(
                    "Attempt to open a mutable message set with a view or offset, which is not allowed.");

            if (needRecover.get()) {
                // set the file position to the end of the file for appending messages
                long startMs = System.currentTimeMillis();
                long truncated = recover();
                logger.info("Recovery succeeded in " + (System.currentTimeMillis() - startMs) / 1000 + " seconds. " + truncated + " bytes truncated.");
            } else {
                setSize.set(channel.size());
                setHighWaterMark.set(getSizeInBytes());
                channel.position(channel.size());
            }
        } else {
            setSize.set(Math.min(channel.size(), limit) - offset);
            setHighWaterMark.set(getSizeInBytes());
        }
    }

    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(FileChannel channel, boolean mutable) throws IOException {
        this(channel, 0, Long.MAX_VALUE, mutable, new AtomicBoolean(false));
    }

    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(File file, boolean mutable) throws IOException {
        this(Utils.openChannel(file, mutable), mutable);
    }

    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(FileChannel channel, boolean mutable, AtomicBoolean needRecover) throws IOException {
        this(channel, 0, Long.MAX_VALUE, mutable, needRecover);
    }

    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(File file, boolean mutable, AtomicBoolean needRecover) throws IOException {
        this(Utils.openChannel(file, mutable), mutable, needRecover);
    }

    public Iterator<MessageAndOffset> iterator() {
        return new IteratorTemplate<MessageAndOffset>() {

            long location = offset;

            @Override
            protected MessageAndOffset makeNext() {
                try {

                    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                    channel.read(sizeBuffer, location);
                    if (sizeBuffer.hasRemaining()) {
                        return allDone();
                    }
                    sizeBuffer.rewind();
                    int size = sizeBuffer.getInt();
                    if (size < Message.MinHeaderSize) {
                        return allDone();
                    }
                    ByteBuffer buffer = ByteBuffer.allocate(size);
                    channel.read(buffer, location + 4);
                    if (buffer.hasRemaining()) {
                        return allDone();
                    }
                    buffer.rewind();
                    location += size + 4;

                    return new MessageAndOffset(new Message(buffer), location);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        };
    }

    /**
     * the max offset(next message id).<br/>
     * The <code> #getSizeInBytes()</code> maybe is larger than {@link #highWaterMark()}
     * while some messages were cached in memory(not flush to disk).
     */
    public long getSizeInBytes() {
        return setSize.get();
    }

    @Override
    public long writeTo(GatheringByteChannel destChannel, long writeOffset, long maxSize) throws IOException {
        return channel.transferTo(offset + writeOffset, Math.min(maxSize, getSizeInBytes()), destChannel);
    }

    /**
     * read message from file
     *
     * @param readOffset offset in this channel(file);not the message offset
     * @param size       max data size
     * @return messages sharding data with file log
     * @throws IOException reading file failed
     */
    public MessageSet read(long readOffset, long size) throws IOException {
        return new FileMessageSet(channel, this.offset + readOffset, //
                Math.min(this.offset + readOffset + size, highWaterMark()), false, new AtomicBoolean(false));
    }

    /**
     * Append this message to the message set
     *
     * @return the written size and first offset
     * @throws IOException
     */
    public long[] append(MessageSet messages) throws IOException {
        checkMutable();
        long written = 0L;
        while (written < messages.getSizeInBytes())
            written += messages.writeTo(channel, 0, messages.getSizeInBytes());
        long beforeOffset = setSize.getAndAdd(written);
        return new long[]{written, beforeOffset};
    }

    /**
     * Commit all written data to the physical disk
     *
     * @throws IOException
     */
    public void flush() throws IOException {
        checkMutable();
        long startTime = System.currentTimeMillis();
        channel.force(true);
        long elapsedTime = System.currentTimeMillis() - startTime;
        LogFlushStats.recordFlushRequest(elapsedTime);
        logger.debug("flush time " + elapsedTime);
        setHighWaterMark.set(getSizeInBytes());
        logger.debug("flush high water mark:" + highWaterMark());
    }

    /**
     * Close this message set
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (mutable) flush();
        channel.close();
    }

    /**
     * Recover log up to the last complete entry. Truncate off any bytes from any incomplete
     * messages written
     *
     * @throws IOException
     */
    private long recover() throws IOException {
        checkMutable();
        long len = channel.size();
        ByteBuffer buffer = ByteBuffer.allocate(4);
        long validUpTo = 0;
        long next = 0L;
        do {
            next = validateMessage(channel, validUpTo, len, buffer);
            if (next >= 0) validUpTo = next;
        } while (next >= 0);
        channel.truncate(validUpTo);
        setSize.set(validUpTo);
        setHighWaterMark.set(validUpTo);
        logger.info("recover high water mark:" + highWaterMark());
        /* This should not be necessary, but fixes bug 6191269 on some OSs. */
        channel.position(validUpTo);
        needRecover.set(false);
        return len - validUpTo;
    }

    /**
     * Read, validate, and discard a single message, returning the next valid offset, and the
     * message being validated
     *
     * @throws IOException
     */
    private long validateMessage(FileChannel channel, long start, long len, ByteBuffer buffer) throws IOException {
        buffer.rewind();
        int read = channel.read(buffer, start);
        if (read < 4) return -1;

        // check that we have sufficient bytes left in the file
        int size = buffer.getInt(0);
        if (size < Message.MinHeaderSize) return -1;

        long next = start + 4 + size;
        if (next > len) return -1;

        // read the message
        ByteBuffer messageBuffer = ByteBuffer.allocate(size);
        long curr = start + 4;
        while (messageBuffer.hasRemaining()) {
            read = channel.read(messageBuffer, curr);
            if (read < 0) throw new IllegalStateException("File size changed during recovery!");
            else curr += read;
        }
        messageBuffer.rewind();
        Message message = new Message(messageBuffer);
        if (!message.isValid()) return -1;
        else return next;
    }

    void checkMutable() {
        if (!mutable) throw new IllegalStateException("Attempt to invoke mutation on immutable message set.");
    }

    /**
     * The max offset(next message id) persisted in the log file.<br/>
     * Messages with smaller offsets have persisted in file.
     *
     * @return max offset
     */
    public long highWaterMark() {
        return setHighWaterMark.get();
    }

}
