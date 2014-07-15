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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static com.facebook.nifty.client.socks.SocksProtocols.createSock4aPacket;
import static com.facebook.nifty.client.socks.SocksProtocols.createSocks4packet;

/**
 * ClientBootstrap for connecting via SOCKS proxy.
 * Currently only SOCK4 is supported since we don't do authentication anyway.
 * <p/>
 * See http://en.wikipedia.org/wiki/SOCKS
 */
public class Socks4ClientBootstrap extends DelegatingClientBootstrap implements ClientBootstrap
{
    static final String FRAME_DECODER = "frameDecoder";
    static final String HANDSHAKE = "handshake";

    private final InetSocketAddress socksProxyAddr;

    public Socks4ClientBootstrap(InetSocketAddress socksProxyAddr)
    {
        super(new Bootstrap());
        this.socksProxyAddr = socksProxyAddr;
    }

    //public Socks4ClientBootstrap(InetSocketAddress socksProxyAddr)
    //{
    //    this.socksProxyAddr = socksProxyAddr;
    //    //super.setPipeline(getPipeline());
    //}

    /**
     * Hijack super class's pipelineFactory and return our own that
     * does the connect to SOCKS proxy and does the handshake.
     */
    //@Override
    //public <C extends Channel> NiftyChannelInitializer<C> getPipelineFactory()
    //{
    //    return new NiftyChannelInitializer<C>()
    //    {
    //        @Override
    //        public void initChannel(C channel) throws Exception
    //        {
    //            ChannelPipeline cp = channel.pipeline();
    //            cp.addLast(FRAME_DECODER, new FixedLengthFrameDecoder(8));
    //            //cp.addLast(HANDSHAKE, new Socks4HandshakeHandler(Socks4ClientBootstrap.super.getPipelineFactory()));
    //        }
    //    };
    //}

    /**
     * Hijack the connect method to connect to socks proxy and then
     * send the connection handshake once connection to proxy is established.
     *
     * @return returns a ChannelFuture, it will be ready once the connection to
     *         socks and the remote address is established ( i.e. after the handshake completes )
     */
    @Override
    public ChannelFuture connect(final SocketAddress remoteAddress)
    {
        if (!(remoteAddress instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("expecting InetSocketAddress");
        }
        final SettableChannelFuture settableChannelFuture = new SettableChannelFuture();
        super.connect(socksProxyAddr).addListener(new ChannelFutureListener()
        {
            @Override
            public void operationComplete(final ChannelFuture future)
                    throws Exception
            {
                settableChannelFuture.setChannel(future.channel());
                if (future.isSuccess()) {
                    socksConnect(future.channel(), (InetSocketAddress) remoteAddress).addListener(new ChannelFutureListener()
                    {
                        @Override
                        public void operationComplete(ChannelFuture innerFuture)
                                throws Exception
                        {
                            if (innerFuture.isSuccess()) {
                                settableChannelFuture.setSuccess(null);
                            }
                            else {
                                settableChannelFuture.setFailure(innerFuture.cause());
                            }
                        }
                    });
                }
                else {
                    settableChannelFuture.setFailure(future.cause());
                }
            }
        });
        return settableChannelFuture;
    }


    /**
     * try to look at the remoteAddress and decide to use SOCKS4 or SOCKS4a handshake
     * packet.
     */
    private ChannelFuture socksConnect(Channel channel, InetSocketAddress remoteAddress)
    {
        ByteBuf handshake = null;
        if ((remoteAddress.getAddress() == null && remoteAddress.getHostName() != null) || remoteAddress.getHostName().equals("localhost")) {
            handshake = createSock4aPacket(remoteAddress.getHostName(), remoteAddress.getPort());
        }
        if (remoteAddress.getAddress() != null) {
            handshake = createSocks4packet(remoteAddress.getAddress(), remoteAddress.getPort());
        }

        if (handshake == null) {
            throw new IllegalArgumentException("Invalid Address " + remoteAddress);
        }

        channel.write(handshake);
        return ((Socks4HandshakeHandler) channel.pipeline().get("handshake")).getChannelFuture();
    }
}
