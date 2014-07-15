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
import io.netty.buffer.Unpooled;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;

public class TestByteBufTransports
{
    @Test
    public void testReadByteCounter() throws TException
    {
        ByteBuf firstChunk = writeFirstChunk(new ByteBufOutputTransport()).getOutputBuffer();
        int firstChunkSize = firstChunk.readableBytes();

        ByteBuf secondChunk = writeSecondChunk(new ByteBufOutputTransport()).getOutputBuffer();
        int secondChunkSize = secondChunk.readableBytes();

        ByteBuf buf = Unpooled.wrappedBuffer(firstChunk, secondChunk);
        ByteBufInputTransport inputTransport = new ByteBufInputTransport(buf);
        TBinaryProtocol inputProtocol = new TBinaryProtocol(inputTransport);
        assertEquals(inputTransport.getReadByteCount(), 0, "No bytes should have been read yet");

        readFirstChunk(inputProtocol);
        assertEquals(inputTransport.getReadByteCount(), firstChunkSize, "Should have read all bytes from first chunk");

        readSecondChunk(inputProtocol);
        assertEquals(inputTransport.getReadByteCount(), firstChunkSize + secondChunkSize, "Should have read all bytes from second chunk");
    }

    @Test
    public void testWriteByteCounter() throws TException
    {
        ByteBuf firstChunk = writeFirstChunk(new ByteBufOutputTransport()).getOutputBuffer();
        int firstChunkSize = firstChunk.readableBytes();

        ByteBuf secondChunk = writeSecondChunk(new ByteBufOutputTransport()).getOutputBuffer();
        int secondChunkSize = secondChunk.readableBytes();

        ByteBufOutputTransport outputTransport = new ByteBufOutputTransport();

        writeFirstChunk(outputTransport);
        assertEquals(outputTransport.getWrittenByteCount(), firstChunkSize);

        writeSecondChunk(outputTransport);
        assertEquals(outputTransport.getWrittenByteCount(), firstChunkSize + secondChunkSize);
    }

    private <T extends TTransport> T writeFirstChunk(T outTransport) throws TException
    {
        TProtocol outProtocol = new TBinaryProtocol(outTransport);

        outProtocol.writeMessageBegin(new TMessage());
        outProtocol.writeString("hello world");

        return outTransport;
    }

    private <T extends TTransport> T writeSecondChunk(T outTransport) throws TException
    {
        TProtocol outProtocol = new TBinaryProtocol(outTransport);

        outProtocol.writeI32(111);
        outProtocol.writeBinary(ByteBuffer.wrap(new byte[10]));

        return outTransport;
    }

    private void readFirstChunk(TProtocol inProtocol) throws TException
    {
        inProtocol.readMessageBegin();
        inProtocol.readString();
    }

    private void readSecondChunk(TProtocol inProtocol) throws TException
    {
        inProtocol.readI32();
        inProtocol.readBinary();
    }
}
