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

import com.facebook.nifty.client.BootstrapWrapper;
import com.facebook.nifty.client.NiftyChannelInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.socks.SocksAuthScheme;
import io.netty.handler.codec.socks.SocksInitRequest;
import io.netty.handler.codec.socks.SocksInitResponseDecoder;
import io.netty.handler.codec.socks.SocksMessageEncoder;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static com.google.common.collect.Lists.newArrayList;

/**
 * ClientBootstrap for connecting via SOCKS proxy.
 * Currently only SOCKS5 with no authentication is supported.
 * <p/>
 * See http://en.wikipedia.org/wiki/SOCKS
 */
public class SocksClientBootstrapWrapper extends BootstrapWrapper
{
    private final InetSocketAddress socksProxyAddress;
    private NiftyChannelInitializer<SocketChannel> initializationHandler = null;

    public SocksClientBootstrapWrapper(InetSocketAddress socksProxyAddress,
                                       Bootstrap wrappedBootstrap)
    {
        super(wrappedBootstrap);
        this.socksProxyAddress = socksProxyAddress;
    }

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
        ChannelFuture socksConnectFuture = null;
        addInitHandler((InetSocketAddress) remoteAddress);
        socksConnectFuture = getWrappedBootstrap().connect(socksProxyAddress);
        socksConnectFuture.addListener(new ChannelFutureListener()
        {
            @Override
            public void operationComplete(ChannelFuture future)
                    throws Exception
            {
                settableChannelFuture.setChannel(future.channel());
                if (future.isSuccess()) {
                    socksConnect(future.channel(), (InetSocketAddress) remoteAddress).addListener
                            (new ChannelFutureListener()
                            {
                                @Override
                                public void operationComplete(ChannelFuture innerFuture)
                                        throws Exception
                                {
                                    if (innerFuture.isSuccess()) {
                                        settableChannelFuture.setSuccess();
                                    } else {
                                        settableChannelFuture.setFailure(innerFuture.cause());
                                    }
                                }
                            });
                } else {
                    settableChannelFuture.setFailure(future.cause());
                }
            }
        });
        return settableChannelFuture;
    }


    @Override
    public void handler(final NiftyChannelInitializer<SocketChannel> initializationHandler)
    {
        // Don't apply the initialization handler now: wait until we've connected through
        // SOCKS, then apply it
        this.initializationHandler = initializationHandler;
    }

    /**
    * try to look at the remoteAddress and decide to use SOCKS4 or SOCKS4a handshake
    * packet.
    */
    private ChannelFuture socksConnect(Channel channel, InetSocketAddress remoteAddress)
    {
        channel.write(new SocksInitRequest(newArrayList(SocksAuthScheme.NO_AUTH)));
        return ((SocksHandshakeHandler) channel.pipeline().get("HANDSHAKE_HANDLER")).getChannelFuture();
    }

    private void addInitHandler(final InetSocketAddress remoteAddress) {
        getWrappedBootstrap().handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception
            {
                final ChannelPipeline cp = ch.pipeline();
                cp.addLast("MESSAGE_ENCODER", new SocksMessageEncoder());
                cp.addLast("INIT_RESPONSE_DECODER", new SocksInitResponseDecoder());
                cp.addLast("HANDSHAKE_HANDLER", new SocksHandshakeHandler(initializationHandler,
                                                                          remoteAddress));
            }
        });
    }
}
