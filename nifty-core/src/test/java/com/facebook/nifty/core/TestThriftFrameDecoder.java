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

import com.facebook.nifty.codec.DefaultThriftFrameDecoder;
import com.facebook.nifty.codec.ThriftFrameDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.EventLoop;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TFramedTransport;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestThriftFrameDecoder
{
    private Channel channel;
    private AtomicInteger messagesReceived;
    private AtomicInteger exceptionsCaught;

    private static final int MAX_FRAME_SIZE = 1024;
    public static final int MESSAGE_CHUNK_SIZE = 10;

    // Send an empty buffer and make sure nothing breaks, and
    @Test
    public void testDecodeEmptyBuffer() throws Exception
    {
        sendMessage(channel, Unpooled.EMPTY_BUFFER);

        Assert.assertEquals(exceptionsCaught.get(), 0);
        Assert.assertEquals(messagesReceived.get(), 0);
    }

    // Send two unframed messages in a single buffer, and check they both get decoded
    @Test
    public void testDecodeUnframedMessages() throws Exception
    {
        final ByteBufOutputTransport transport = new ByteBufOutputTransport();
        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        writeTestMessages(protocol, 2);

        sendMessage(channel, transport.getOutputBuffer());

        Assert.assertEquals(exceptionsCaught.get(), 0);
        Assert.assertEquals(messagesReceived.get(), 2);
    }

    // Send two framed messages in a single buffer, and check they both get decoded
    @Test
    public void testDecodeFramedMessages() throws Exception
    {
        final ByteBufOutputTransport transport = new ByteBufOutputTransport();
        TBinaryProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));

        writeTestMessages(protocol, 2);

        sendMessage(channel, transport.getOutputBuffer());

        Assert.assertEquals(messagesReceived.get(), 2);
    }

    // Send three unframed messages, chunked into 10-byte buffers and make sure they all get decoded
    @Test
    public void testDecodeChunkedUnframedMessages() throws Exception
    {
        final ByteBufOutputTransport transport = new ByteBufOutputTransport();
        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        writeTestMessages(protocol, 3);

        sendMessagesInChunks(channel, transport.getOutputBuffer(), MESSAGE_CHUNK_SIZE);

        Assert.assertEquals(messagesReceived.get(), 3);
    }

    // Send three framed messages, chunked into 10-byte buffers and make sure they all get decoded
    @Test
    public void testDecodeChunkedFramedMessages() throws Exception
    {
        ByteBufOutputTransport transport = new ByteBufOutputTransport();
        TBinaryProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));

        writeTestMessages(protocol, 3);
        int totalSize = transport.getOutputBuffer().readableBytes();

        sendMessagesInChunks(channel, transport.getOutputBuffer(), totalSize / 7);

        Assert.assertEquals(messagesReceived.get(), 3);
    }

    private void sendMessage(final Channel channel,
                             final ByteBuf message) throws ExecutionException, InterruptedException
    {
        channel.eventLoop().submit(new Runnable() {
            @Override
            public void run()
            {
                channel.pipeline().fireChannelRead(message);
            }
        }).get();
    }

    private void sendMessagesInChunks(final Channel channel,
                                      final ByteBuf message,
                                      final int chunkSize) throws ExecutionException, InterruptedException
    {
        channel.eventLoop().submit(new Runnable() {
            @Override
            public void run()
            {
                ByteBuf buffer = message;
                while (buffer.readableBytes() > 0) {
                    // TODO(NETTY4): see if this should work, and file a bug if it should
                    //final ByteBuf chunk = buffer.readSlice(Math.min(chunkSize, buffer.readableBytes()));

                    final ByteBuf chunk = buffer.copy(buffer.readerIndex(), Math.min(chunkSize, buffer.readableBytes()));
                    buffer.skipBytes(chunk.readableBytes());

                    channel.pipeline().fireChannelRead(chunk);
                }
            }
        }).get();
    }

    private void writeTestMessages(TBinaryProtocol protocol, int count)
            throws TException
    {
        for (int i = 0; i < count; i++) {
            protocol.writeMessageBegin(new TMessage("testmessage" + i, TMessageType.CALL, i));
            {
                protocol.writeStructBegin(new TStruct());
                {
                    protocol.writeFieldBegin(new TField("i32field", TType.I32, (short) 1));
                    protocol.writeI32(123);
                    protocol.writeFieldEnd();
                }
                {
                    protocol.writeFieldBegin(new TField("strfield", TType.STRING, (short) 2));
                    protocol.writeString("foo");
                    protocol.writeFieldEnd();
                }
                {
                    protocol.writeFieldBegin(new TField("boolfield", TType.BOOL, (short) 3));
                    protocol.writeBool(true);
                    protocol.writeFieldEnd();
                }
                protocol.writeFieldStop();
                protocol.writeStructEnd();
            }
            protocol.writeMessageEnd();
            protocol.getTransport().flush();
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
        ThriftFrameDecoder decoder = new DefaultThriftFrameDecoder(MAX_FRAME_SIZE,
                                                                   new TBinaryProtocol.Factory());
        exceptionsCaught = new AtomicInteger(0);
        messagesReceived = new AtomicInteger(0);

        channel = new LocalChannel();
        new NioEventLoopGroup(1).next().register(channel);

        channel.pipeline().addLast(
                decoder,
                new ChannelInboundHandlerAdapter()
                {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
                    {
                        messagesReceived.incrementAndGet();
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
                    {
                        exceptionsCaught.incrementAndGet();
                    }
                }
        );
    }
}
