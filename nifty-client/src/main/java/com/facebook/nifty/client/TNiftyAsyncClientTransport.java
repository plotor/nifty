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
package com.facebook.nifty.client;

import io.netty.buffer.MessageBuf;
import io.netty.channel.ChannelInboundMessageHandler;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This already has a built in TFramedTransport. No need to wrap.
 */
@NotThreadSafe
public class TNiftyAsyncClientTransport extends TTransport implements ChannelInboundMessageHandler<ByteBuf>
{
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    // this is largely a guess. there shouldn't really be more than 2 write buffers at any given time.
    private static final int MAX_BUFFERS_IN_QUEUE = 3;
    private final Channel channel;
    private final Queue<ByteBuf> writeBuffers;
    private volatile TNiftyClientListener listener;
    private final ChannelInboundMessageHandlerAdapter<ByteBuf> delegate =
            new ChannelInboundMessageHandlerAdapter<ByteBuf>()
            {
                @Override
                public void messageReceived(ChannelHandlerContext ctx, ByteBuf msg) throws Exception
                {
                    TNiftyAsyncClientTransport.this.messageReceived(ctx, msg);
                }
            };

    public TNiftyAsyncClientTransport(Channel channel)
    {
        this.channel = channel;
        this.writeBuffers = new ConcurrentLinkedQueue<>();
    }

    public void setListener(TNiftyClientListener listener)
    {
        this.listener = listener;
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public void open()
            throws TTransportException
    {
        // no-op
    }

    @Override
    public void close()
    {
        channel.close();
    }

    @Override
    public int read(byte[] bytes, int offset, int length)
            throws TTransportException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
            throws TTransportException
    {
        getWriteBuffer().writeBytes(bytes, offset, length);
    }

    @Override
    public void flush()
            throws TTransportException
    {
        // all these is to re-use the write buffer. We can only clear
        // and re-use a write buffer when the write operation completes,
        // which is an async operation in netty. the future listener
        // down here will be invoked by Netty I/O thread.
        if (!writeBuffers.isEmpty()) {
            channel.write(writeBuffers.remove());
        }
    }

    public ByteBuf getWriteBuffer()
    {
        if (writeBuffers.isEmpty())
        {
            writeBuffers.add(channel.alloc().buffer(DEFAULT_BUFFER_SIZE));
        }
        return writeBuffers.peek();
    }

    /// delegation methods with added functionality

    private void messageReceived(ChannelHandlerContext ctx, ByteBuf in)
            throws Exception
    {
        listener.onFrameRead(ctx.channel(), in);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        listener.onChannelClosedOrDisconnected(ctx.channel());
        delegate.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        listener.onExceptionEvent(cause);
        delegate.exceptionCaught(ctx, cause);
    }

    /// delegation-only methods

    @Override
    public MessageBuf<ByteBuf> newInboundBuffer(ChannelHandlerContext ctx) throws Exception
    {
        return delegate.newInboundBuffer(ctx);
    }

    @Override
    public void freeInboundBuffer(ChannelHandlerContext ctx) throws Exception
    {
        delegate.freeInboundBuffer(ctx);
    }

    @Override
    public void inboundBufferUpdated(ChannelHandlerContext ctx) throws Exception
    {
        delegate.inboundBufferUpdated(ctx);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception
    {
        delegate.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception
    {
        delegate.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        delegate.channelActive(ctx);
    }

    @Override
    public void channelReadSuspended(ChannelHandlerContext ctx) throws Exception
    {
        delegate.channelReadSuspended(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
    {
        delegate.userEventTriggered(ctx, evt);
    }

    @Override
    public void beforeAdd(ChannelHandlerContext ctx) throws Exception
    {
        delegate.beforeAdd(ctx);
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception
    {
        delegate.afterAdd(ctx);
    }

    @Override
    public void beforeRemove(ChannelHandlerContext ctx) throws Exception
    {
        delegate.beforeRemove(ctx);
    }

    @Override
    public void afterRemove(ChannelHandlerContext ctx) throws Exception
    {
        delegate.afterRemove(ctx);
    }
}
