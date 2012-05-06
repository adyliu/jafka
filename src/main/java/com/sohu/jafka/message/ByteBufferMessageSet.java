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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.common.InvalidMessageSizeException;
import com.sohu.jafka.common.MessageSizeTooLargeException;
import com.sohu.jafka.utils.IteratorTemplate;

/**
 * A sequence of messages stored in a byte buffer
 * 
 * There are two ways to create a ByteBufferMessageSet
 * 
 * Option 1: From a ByteBuffer which already contains the serialized
 * message set. Consumers will use this method.
 * 
 * Option 2: Give it a list of messages along with instructions relating to
 * serialization format. Producers will use this method.
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ByteBufferMessageSet extends MessageSet{

    private final ByteBuffer buffer;
    private final long initialOffset;
    private final ErrorMapping errorCode;
    //
    private long shallowValidByteCount = -1L;
    //
    private long validBytes;
    public ByteBufferMessageSet(ByteBuffer buffer) {
        this(buffer,0L,ErrorMapping.NoError);
    }
    public ByteBufferMessageSet(ByteBuffer buffer,long initialOffset,ErrorMapping errorCode) {
        this.buffer = buffer;
        this.initialOffset = initialOffset;
        this.errorCode = errorCode;
        this.validBytes = shallowValidBytes();
    }
    
    public ByteBufferMessageSet(CompressionCodec compressionCodec,Message...messages) {
        this(MessageSet.createByteBuffer(compressionCodec, messages),0L,ErrorMapping.NoError);
    }
    public ByteBufferMessageSet(Message...messages) {
        this(CompressionCodec.NoCompressionCodec,messages);
    }
    /** get valid bytes of buffer
     * <p>
     * The size of buffer is equal or larger than the size of valid messages.
     * The last message maybe is not integrate.
     * </p>
     * @return the validBytes 
     */
    public long getValidBytes() {
        return validBytes;
    }
    
    private long shallowValidBytes() {
        if (shallowValidByteCount < 0) {
            Iterator<MessageAndOffset> iter = this.internalIterator(true);
            while (iter.hasNext()) {
                shallowValidByteCount = iter.next().offset;
            }
        }
        if (shallowValidByteCount < initialOffset) {
            return 0;
        } else {
            return shallowValidByteCount - initialOffset;
        }
    }
    /**
     * @return the initialOffset
     */
    public long getInitialOffset() {
        return initialOffset;
    }
    
    /**
     * @return the buffer
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }
    
    /**
     * @return the errorCode
     */
    public ErrorMapping getErrorCode() {
        return errorCode;
    }
    public ByteBuffer serialized() {
        return buffer;
    }
    public Iterator<MessageAndOffset> iterator() {
        return internalIterator(false);
    }
    
    public Iterator<MessageAndOffset> internalIterator(boolean isShallow){
        return new Iter(isShallow);
    }
    
    class Iter extends IteratorTemplate<MessageAndOffset> {

        boolean isShallow;
        ByteBuffer topIter = buffer.slice();
        long currValidBytes = initialOffset;
        Iterator<MessageAndOffset> innerIter = null;
        long lastMessageSize = 0L;
        Iter(boolean isShallow) {
            this.isShallow = isShallow;
        }
        
        private boolean innerDone() {
            return innerIter == null || !innerIter.hasNext();
        }

        private MessageAndOffset makeNextOuter() {
            if(topIter.remaining() <4)return allDone();
            int size = topIter.getInt();
            lastMessageSize = size;
            if(size<0||topIter.remaining()<size) {
                if(currValidBytes == initialOffset||size<0) {
                    throw new InvalidMessageSizeException("invalid message size: " + size + " only received bytes: " +
                            topIter.remaining() + " at " + currValidBytes + "( possible causes (1) a single message larger than " +
                            "the fetch size; (2) log corruption )");
                }
                return allDone();
            }
            //
            ByteBuffer message = topIter.slice();
            message.limit(size);
            topIter.position(topIter.position() + size);
            Message newMessage = new Message(message);
            if(isShallow) {
                currValidBytes += 4 +size;
                return new MessageAndOffset(newMessage, currValidBytes);
            }
            if(newMessage.compressionCodec() == CompressionCodec.NoCompressionCodec) {
                if(!newMessage.isValid())throw new InvalidMessageException("Uncompressed essage is invalid");
                innerIter = null;
                currValidBytes += 4 +size;
                return new MessageAndOffset(newMessage, currValidBytes);
            }
            //compress message
            if(!newMessage.isValid()) {
                throw new InvalidMessageException("Compressed message is invalid");
            }
            innerIter = CompressionUtils.decompress(newMessage).internalIterator(false);
            if(!innerIter.hasNext()) {
                currValidBytes += 4 + lastMessageSize;
                innerIter = null;
            }
            return makeNext();
        }

        @Override
        protected MessageAndOffset makeNext() {
            if (isShallow) return makeNextOuter();
            if (innerDone()) return makeNextOuter();
            MessageAndOffset messageAndOffset = innerIter.next();
            if (!innerIter.hasNext()) {
                currValidBytes += 4 + lastMessageSize;
            }
            return new MessageAndOffset(messageAndOffset.message, currValidBytes);
        }

    }
    
    @Override
    public long writeTo(GatheringByteChannel channel, long offset, long maxSize) throws IOException {
        buffer.mark();
        int written = channel.write(buffer);
        buffer.reset();
        return written;
    }
    public long getSizeInBytes() {
        return buffer.limit();
    }
    /**
     * check max size of each message
     * @param maxMessageSize the max size for each message
     */
    public void verifyMessageSize(int maxMessageSize) {
       Iterator<MessageAndOffset> shallowIter =  internalIterator(true);
       while(shallowIter.hasNext()) {
           MessageAndOffset messageAndOffset = shallowIter.next();
           int payloadSize = messageAndOffset.message.payloadSize();
           if(payloadSize > maxMessageSize) {
               throw new MessageSizeTooLargeException("payload size of " + payloadSize + " larger than " + maxMessageSize);
           }
       }
    }
    
    
}
