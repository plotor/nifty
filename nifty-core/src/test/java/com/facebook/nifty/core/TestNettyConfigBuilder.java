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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannelConfig;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;

public class TestNettyConfigBuilder
{
    @Test
    public void testNettyConfigBuilder() throws Exception
    {
        final NettyServerConfigBuilder configBuilder = new NettyServerConfigBuilder();

        configBuilder.getServerSocketChannelConfig().setReceiveBufferSize(10000);
        configBuilder.getServerSocketChannelConfig().setBacklog(1000);
        configBuilder.getServerSocketChannelConfig().setReuseAddress(true);

        NettyServerConfig config = configBuilder.build();

        Channel serverChannel = null;

        try {
            final ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.channel(NioServerSocketChannel.class);
            ChannelFuture serverChannelFuture = bootstrap.group(new NioEventLoopGroup(1))
                                                         .handler(NiftyChannelInitializers.getOptionsInitializer(config.getChannelOptions()))
                                                         .childHandler(NiftyChannelInitializers.getOptionsInitializer(config.getChildChannelOptions()))
                                                         .bind(new InetSocketAddress(0));

            serverChannelFuture.get();
            serverChannel = serverChannelFuture.channel();

            Assert.assertEquals(((ServerSocketChannelConfig) serverChannel.config()).getReceiveBufferSize(), 10000);
            Assert.assertEquals(((ServerSocketChannelConfig) serverChannel.config()).getBacklog(), 1000);
            Assert.assertTrue(((ServerSocketChannelConfig) serverChannel.config()).isReuseAddress());
        }
        finally {
            if (serverChannel != null) {
                serverChannel.close();
            }
        }
    }
}
