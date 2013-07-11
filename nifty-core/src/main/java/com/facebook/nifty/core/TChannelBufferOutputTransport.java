/*
 * Copyright (C) 2012-2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.nifty.core;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link TTransport} that buffers the output of a single message,
 * so that an async client can grab the buffer and send it
 */
@NotThreadSafe
public class TChannelBufferOutputTransport extends TTransport
{
    private static final int DEFAULT_INITIAL_SIZE = 1024;

    // This ratio defines "underuse" of the allocated buffer. For example if this ratio is 0.75,
    // flushing a message whose length is less than 3/4 of the buffer will count as under-utilized.
    private static final float UNDERUSE_RATIO = 0.75f;

    // This threshold sets how many times the buffer must be under-utilized before we'll reallocate
    // it to the initial size.
    private static final int UNDERUSE_THRESHOLD = 5;

    private ChannelBuffer outputBuffer;
    private final int initialSize;
    private int bufferUnderuseCounter;

    public TChannelBufferOutputTransport()
    {
        this.initialSize = DEFAULT_INITIAL_SIZE;
        outputBuffer = ChannelBuffers.dynamicBuffer(this.initialSize);
    }

    public TChannelBufferOutputTransport(int initialSize)
    {
        this.initialSize = initialSize;
        outputBuffer = ChannelBuffers.dynamicBuffer(this.initialSize);
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void open() throws TTransportException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException
    {
        outputBuffer.writeBytes(buf, off, len);
    }

    @Override
    public void flush()
    {
        if (outputBuffer.writerIndex() < (int)(outputBuffer.capacity() * UNDERUSE_RATIO)) {
            ++bufferUnderuseCounter;
        }
        else {
            bufferUnderuseCounter = 0;
        }
        resetOutputBuffer();
    }

    public void resetOutputBuffer()
    {
        if (shouldShrinkBuffer()) {
            outputBuffer = ChannelBuffers.dynamicBuffer(initialSize);
            bufferUnderuseCounter = 0;
        } else {
            outputBuffer.clear();
        }
    }

    public ChannelBuffer getOutputBuffer()
    {
        return outputBuffer;
    }

    private boolean shouldShrinkBuffer()
    {
        // We want to shrink the buffer if it has been under-utilized UNDERUSE_THRESHOLD
        // times in a row, and the size after shrinking (the initial size defined for this
        // transport) is less than UNDERUSE_RATIO of the current size.
        return (bufferUnderuseCounter > UNDERUSE_THRESHOLD) &&
               (initialSize < (int)(outputBuffer.capacity() * UNDERUSE_RATIO));
    }
}
