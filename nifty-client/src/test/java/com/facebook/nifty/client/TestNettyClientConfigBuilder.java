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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static com.facebook.nifty.core.NiftyChannelInitializers.emptyChannelInitializer;

public class TestNettyClientConfigBuilder
{
    @Test
    public void testNettyClientConfigBuilder() throws ExecutionException, InterruptedException
    {
        final NettyClientConfigBuilder configBuilder = new NettyClientConfigBuilder();

        configBuilder.getSocketChannelConfig().setWriteSpinCount(5);
        configBuilder.getSocketChannelConfig().setTcpNoDelay(false);
        configBuilder.getSocketChannelConfig().setConnectTimeoutMillis(100);

        NettyClientConfig config = configBuilder.build();

        NioServerSocketChannel serverChannel = null;
        NioSocketChannel clientChannel = null;

        try {
            serverChannel = startServer();

            int port = serverChannel.localAddress().getPort();

            final DefaultClientBootstrap bootstrap = new DefaultClientBootstrap();
            ChannelFuture clientChannelFuture = bootstrap.group(new NioEventLoopGroup(1))
                                                         .setOptions(config.getBootstrapOptions())
                                                         .connect(new InetSocketAddress("localhost", port));

            clientChannel = (NioSocketChannel) clientChannelFuture.channel();
            clientChannelFuture.get();

            Assert.assertEquals(clientChannel.config().getWriteSpinCount(), 5);
            Assert.assertEquals(clientChannel.config().getConnectTimeoutMillis(), 100);
            Assert.assertFalse(clientChannel.config().isTcpNoDelay());
        }
        finally {
            if (serverChannel != null) {
                serverChannel.close();
            }
            if (clientChannel != null) {
                clientChannel.close();
            }
        }
    }

    private NioServerSocketChannel startServer() throws InterruptedException, ExecutionException
    {
        ChannelFuture serverChannelFuture =
                new ServerBootstrap().group(new NioEventLoopGroup(1))
                                     .channel(NioServerSocketChannel.class)
                                     .handler(emptyChannelInitializer())
                                     .childHandler(emptyChannelInitializer())
                                     .bind(0);

        serverChannelFuture.get();
        NioServerSocketChannel serverChannel = (NioServerSocketChannel) serverChannelFuture.channel();
        return serverChannel;
    }
}
