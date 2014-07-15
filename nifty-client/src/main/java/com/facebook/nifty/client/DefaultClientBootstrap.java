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

import com.facebook.nifty.core.NiftyChannelInitializers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.SocketAddress;
import java.util.Map;

@NotThreadSafe
public class DefaultClientBootstrap implements ClientBootstrap
{
    private final Bootstrap bootstrap;
    //private NiftyChannelInitializer<Channel> channelInitializer;
    private ImmutableMap.Builder<String, Object> bootstrapOptionsBuilder = ImmutableMap.builder();

    public DefaultClientBootstrap()
    {
        this.bootstrap = new Bootstrap();
        this.bootstrap.channel(NioSocketChannel.class);
    }

    @Override
    public DefaultClientBootstrap setOption(String option, Object value)
    {
        this.bootstrapOptionsBuilder.put(option, value);
        return this;
    }

    @Override
    public DefaultClientBootstrap setOptions(Map<String, Object> bootstrapOptions)
    {
        this.bootstrapOptionsBuilder = ImmutableMap.<String, Object>builder().putAll(bootstrapOptions);
        return this;
    }

    @Override
    public ClientBootstrap handler(ChannelHandler handler)
    {
        bootstrap.handler(handler);
        return this;
    }

    @Override
    public ChannelFuture connect(SocketAddress address)
    {
        bootstrap.handler(NiftyChannelInitializers.getOptionsInitializer(bootstrapOptionsBuilder.build()));
        return bootstrap.connect(address);
    }

    @Override
    public ClientBootstrap group(NioEventLoopGroup group)
    {
        bootstrap.group(group);
        return this;
    }
}
