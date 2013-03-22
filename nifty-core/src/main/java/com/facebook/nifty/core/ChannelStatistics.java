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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerUtil;
import io.netty.channel.ChannelOutboundByteHandler;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelStateHandler;
import io.netty.channel.ChannelStateHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters for number of channels open, generic traffic stats and maybe cleanup logic here.
 */
public class ChannelStatistics extends ChannelDuplexHandler implements ChannelOutboundByteHandler
{
    // TODO : expose these stats somewhere
    private static final AtomicInteger channelCount = new AtomicInteger(0);
    private final AtomicLong bytesRead = new AtomicLong(0);
    private final AtomicLong bytesWritten = new AtomicLong(0);
    private final ChannelGroup allChannels;

    public static final String NAME = ChannelStatistics.class.getSimpleName();

    public ChannelStatistics(ChannelGroup allChannels)
    {
        this.allChannels = allChannels;
    }

    public static int getChannelCount()
    {
        return channelCount.get();
    }

    public long getBytesRead()
    {
        return bytesRead.get();
    }

    public long getBytesWritten()
    {
        return bytesWritten.get();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception
    {
        Preconditions.checkState(!allChannels.contains(ctx.channel()));

        // connect
        channelCount.incrementAndGet();
        allChannels.add(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        // disconnect
        channelCount.decrementAndGet();
        allChannels.remove(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void inboundBufferUpdated(ChannelHandlerContext ctx) throws Exception
    {
        bytesRead.getAndAdd(ctx.nextInboundByteBuffer().readableBytes());
        ctx.fireInboundBufferUpdated();
    }

    @Override
    public void flush(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception
    {
        bytesWritten.getAndAdd(ctx.outboundByteBuffer().readableBytes());
        ctx.nextOutboundByteBuffer().writeBytes(ctx.outboundByteBuffer());
        ctx.flush(promise);
    }

    @Override
    public ByteBuf newOutboundBuffer(ChannelHandlerContext ctx) throws Exception
    {
        return ChannelHandlerUtil.allocate(ctx);
    }

    @Override
    public void freeOutboundBuffer(ChannelHandlerContext ctx) throws Exception
    {
        ctx.outboundByteBuffer().release();
    }

    @Override
    public void discardOutboundReadBytes(ChannelHandlerContext ctx) throws Exception
    {
        ctx.outboundByteBuffer().discardSomeReadBytes();
    }
}
