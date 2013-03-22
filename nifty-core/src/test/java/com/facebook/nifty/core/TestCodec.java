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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.thrift.transport.TTransport;
import org.easymock.EasyMock;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCodec
{
    @Test
    public void testDecoder()
    {
        NettyThriftDecoder decoder = new NettyThriftDecoder();
        ChannelHandlerContext ctx = EasyMock.createMock(ChannelHandlerContext.class);
        Channel channel = EasyMock.createMock(Channel.class);
        EasyMock.expect(channel.alloc()).andReturn(new PooledByteBufAllocator());
        EasyMock.expect(ctx.channel()).andReturn(channel);
        EasyMock.replay(ctx, channel);
        try {
            Object t = decoder.decode(ctx, Unpooled.buffer());
            Assert.assertTrue(t == null);

            ByteBuf channelBuffer = Unpooled.buffer();
            channelBuffer.writeBytes(new byte[16], 0, 16);
            Object t1 = decoder.decode(ctx, channelBuffer);
            Assert.assertTrue(t1 instanceof TTransport);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
