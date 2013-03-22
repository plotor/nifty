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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannelConfig;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class TestNettyConfigBuilder
{
    private int port;

    @BeforeTest(alwaysRun = true)
    public void setup()
    {
        try {
            ServerSocket s = new ServerSocket();
            s.bind(new InetSocketAddress(0));
            port = s.getLocalPort();
            s.close();
        }
        catch (IOException e) {
            port = 8080;
        }
    }

    @Test
    public void testNettyConfigBuilder() throws InterruptedException
    {
        NettyConfigBuilder configBuilder = new NettyConfigBuilder();

        configBuilder.getServerSocketChannelConfig().setReceiveBufferSize(10000);
        configBuilder.getServerSocketChannelConfig().setBacklog(1000);
        configBuilder.getServerSocketChannelConfig().setReuseAddress(true);

        ServerBootstrap bootstrap = new ServerBootstrap();
        configBuilder.applyConfig(bootstrap);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.group(new NioEventLoopGroup(), new NioEventLoopGroup());
        bootstrap.childHandler(new ChannelInitializer<Channel>()
        {
            @Override
            protected void initChannel(Channel ch) throws Exception {}
        });
        Channel serverChannel = bootstrap.bind(new InetSocketAddress(port)).sync().channel();

        Assert.assertEquals(((ServerSocketChannelConfig) serverChannel.config()).getReceiveBufferSize(), 10000);
        Assert.assertEquals(((ServerSocketChannelConfig) serverChannel.config()).getBacklog(), 1000);
        Assert.assertTrue(((ServerSocketChannelConfig) serverChannel.config()).isReuseAddress());
    }
}
