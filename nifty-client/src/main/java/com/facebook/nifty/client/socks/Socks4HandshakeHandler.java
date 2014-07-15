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
package com.facebook.nifty.client.socks;

import com.facebook.nifty.core.NiftyChannelInitializer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * This handshake handler swap out the channel pipeline with the delegate
 * upon handshake completion.
 */
public class Socks4HandshakeHandler extends ChannelInboundHandlerAdapter
{
    private final SettableChannelFuture channelFuture = new SettableChannelFuture();
    //private final ChannelPipelineFactory delegate;

    public <C extends Channel> Socks4HandshakeHandler(NiftyChannelInitializer<C> delegate)
    {
        //this.delegate = delegate;
    }

    public SettableChannelFuture getChannelFuture()
    {
        return channelFuture;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        //channelFuture.setChannel(ctx.channel());
        //if (msg instanceof ByteBuf) {
        //    ByteBuf msg = (ByteBuf) e.getMessage();
        //    if (msg.readableBytes() < 8) {
        //        channelFuture.getDefaultChannelPromise().setFailure(new IOException("invalid sock server reply length = " + msg.readableBytes()));
        //    }
        //    // ignore
        //    msg.readByte();
        //    int status = msg.readByte();
        //    int port = msg.readShort();
        //    byte[] addr = new byte[4];
        //    msg.readBytes(addr);
        //
        //    ctx.channel().setAttachment(new InetSocketAddress(InetAddress.getByAddress(addr), port));
        //
        //    if (status == SocksProtocols.REQUEST_GRANTED) {
        //        ctx.pipeline().remove(Socks4ClientBootstrap.FRAME_DECODER);
        //        ctx.pipeline().remove(Socks4ClientBootstrap.HANDSHAKE);
        //        ChannelPipeline delegatePipeline = delegate.getPipeline();
        //        for (String name : delegatePipeline.names()) {
        //            ctx.pipeline().addLast(name, delegatePipeline.get(name));
        //        }
        //        channelFuture.setSuccess();
        //    }
        //    else {
        //        channelFuture.setFailure(new IOException("sock server reply failure code :" + Integer.toHexString(status)));
        //    }
        //}
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception
    {
        channelFuture.setChannel(ctx.channel());
        channelFuture.setFailure(cause);
    }
}
