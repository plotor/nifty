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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Counters for number of channels open, generic traffic stats and maybe cleanup logic here.
 */
@ChannelHandler.Sharable
public class ChannelStatistics extends ChannelDuplexHandler implements NiftyMetrics
{
    private final AtomicInteger channelCount = new AtomicInteger(0);
    private final AtomicLong bytesRead = new AtomicLong(0);
    private final AtomicLong bytesWritten = new AtomicLong(0);
    private final ChannelGroup allChannels;

    public static final String NAME = ChannelStatistics.class.getSimpleName();

    public ChannelStatistics(ChannelGroup allChannels)
    {
        this.allChannels = allChannels;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        channelCount.incrementAndGet();
        allChannels.add(ctx.channel());

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        channelCount.decrementAndGet();
        allChannels.remove(ctx.channel());

        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        checkArgument(msg instanceof ByteBuf);

        if (msg instanceof ByteBuf) {
            ByteBuf buffer = (ByteBuf) msg;
            int readableBytes = buffer.readableBytes();
            // compute stats here, bytes read from remote
            bytesRead.getAndAdd(readableBytes);
        }

        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        checkArgument(msg instanceof ByteBuf);

        if (msg instanceof ByteBuf) {
            ByteBuf buffer = (ByteBuf) msg;
            int readableBytes = buffer.readableBytes();
            // compute stats here, bytes written to remote
            bytesWritten.getAndAdd(readableBytes);
        }

        super.write(ctx, msg, promise);
    }

    public int getChannelCount()
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
}



