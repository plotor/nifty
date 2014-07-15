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

import io.netty.buffer.ByteBuf;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of {@link TTransport} that wraps an incoming message received so that a
 * {@link org.apache.thrift.protocol.TProtocol} can * be constructed around the wrapper to read
 * the message.
 *
 * Allows for reusing the same transport to read multiple messages via
 * {@link com.facebook.nifty.core.ByteBufInputTransport#setInputBuffer(ByteBuf)}
 */
@NotThreadSafe
public class ByteBufInputTransport extends TTransport {
    private ByteBuf inputBuffer;
    private int initalReaderIndex;

    public ByteBufInputTransport() {
        this.inputBuffer = null;
    }

    public ByteBufInputTransport(ByteBuf inputBuffer) {
        setInputBuffer(inputBuffer);
    }

    @Override
    public boolean isOpen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void open() throws TTransportException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        checkState(inputBuffer != null, "Tried to read before setting an input buffer");
        inputBuffer.readBytes(buf, off, len);
        return len;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        throw new UnsupportedOperationException();
    }

    public void setInputBuffer(ByteBuf inputBuffer) {
        this.initalReaderIndex = inputBuffer.readerIndex();
        this.inputBuffer = inputBuffer;
    }

    public boolean isReadable() {
        return inputBuffer.isReadable();
    }

    public int getReadByteCount()
    {
        return inputBuffer.readerIndex() - initalReaderIndex;
    }
}
