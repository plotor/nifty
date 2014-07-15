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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This already has a built in TFramedTransport. No need to wrap.
 */
@NotThreadSafe
class TNiftyAsyncClientTransport extends TTransport
{
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    // this is largely a guess. there shouldn't really be more than 2 write buffers at any given time.
    private static final int MAX_BUFFERS_IN_QUEUE = 3;
    private final Channel channel;
    private final Queue<ByteBuf> writeBuffers;
    private volatile TNiftyClientListener listener;

    public TNiftyAsyncClientTransport(Channel channel)
    {
        this.channel = channel;
        this.writeBuffers = new ConcurrentLinkedQueue<ByteBuf>();
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
            final ByteBuf channelBuffer = writeBuffers.remove();
            channel.write(channelBuffer).addListener(
                    new ChannelFutureListener()
                    {
                        @Override
                        public void operationComplete(ChannelFuture future)
                                throws Exception
                        {
                            if (future.isSuccess()) {
                                channelBuffer.clear();
                                if (writeBuffers.size() < MAX_BUFFERS_IN_QUEUE) {
                                    writeBuffers.add(channelBuffer);
                                }
                            }
                        }
                    }
            );
        }
    }

    public ByteBuf getWriteBuffer()
    {
        if (writeBuffers.isEmpty()) {
            writeBuffers.add(PooledByteBufAllocator.DEFAULT.buffer(DEFAULT_BUFFER_SIZE));
        }
        return writeBuffers.peek();
    }

    public ChannelHandler getChannelHandler()
    {
        return new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception
            {
                listener.onChannelClosedOrDisconnected(ctx.channel());
                super.channelInactive(ctx);
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
            {
                if (msg instanceof ByteBuf && listener != null) {
                    listener.onFrameRead(ctx.channel(), (ByteBuf) msg);
                }
                // drop it
                super.channelRead(ctx, msg);
            }
        };
    }
}
