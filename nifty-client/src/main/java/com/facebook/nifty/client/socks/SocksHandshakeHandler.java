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

import com.facebook.nifty.client.NiftyChannelInitializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.socks.SocksAddressType;
import io.netty.handler.codec.socks.SocksAuthScheme;
import io.netty.handler.codec.socks.SocksCmdRequest;
import io.netty.handler.codec.socks.SocksCmdResponse;
import io.netty.handler.codec.socks.SocksCmdResponseDecoder;
import io.netty.handler.codec.socks.SocksCmdStatus;
import io.netty.handler.codec.socks.SocksCmdType;
import io.netty.handler.codec.socks.SocksInitResponse;
import io.netty.handler.codec.socks.SocksResponse;

import java.net.InetSocketAddress;

public class SocksHandshakeHandler extends ChannelInboundMessageHandlerAdapter<SocksResponse>
{
    private final SettableChannelFuture channelFuture = new SettableChannelFuture();
    private final NiftyChannelInitializer<SocketChannel> delegateChannelInitializer;
    private final InetSocketAddress address;

    public SocksHandshakeHandler(NiftyChannelInitializer<SocketChannel> delegateChannelInitializer,
                                 InetSocketAddress address)
    {
        this.delegateChannelInitializer = delegateChannelInitializer;
        this.address = address;
    }

    public SettableChannelFuture getChannelFuture()
    {
        return channelFuture;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, SocksResponse msg) throws Exception
    {
        switch (msg.responseType()) {
            case INIT: {
                SocksInitResponse response = (SocksInitResponse) msg;
                ctx.pipeline().addAfter("MESSAGE_ENCODER",
                                        "CMD_RESPONSE_DECODER",
                                        new SocksCmdResponseDecoder());

                if (response.authScheme() == SocksAuthScheme.NO_AUTH) {
                    // Server supports NO_AUTH scheme, we can now send a CONNECT
                    ctx.write(new SocksCmdRequest(SocksCmdType.CONNECT,
                                                  SocksAddressType.IPv4,
                                                  address.getAddress().getHostAddress(),
                                                  address.getPort()));
                }
                else {
                    channelFuture.setFailure(
                            new Exception("NO_AUTH is not supported by your SOCKS server"));
                }
                break;
            }

            case CMD: {
                SocksCmdResponse response = (SocksCmdResponse) msg;
                while (ctx.pipeline().last() != null) {
                    ctx.pipeline().removeLast();
                }
                channelFuture.setChannel(ctx.channel());
                if (response.cmdStatus() == SocksCmdStatus.SUCCESS) {
                    // SOCKS server accepted the CONNECT and will now start proxying traffic.
                    // Run the channel initializer.
                    delegateChannelInitializer.initChannel((SocketChannel) ctx.channel());
                    channelFuture.setSuccess();
                }
                else {
                    channelFuture.setFailure(
                            new Exception("SOCKS connection was unsuccessful"));
                }
                break;
            }

            default:
                channelFuture.setFailure(new Exception("Received unknown SOCKS response"));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e)
            throws Exception
    {
        channelFuture.setChannel(ctx.channel());
        channelFuture.setFailure(e.getCause());
    }
}
