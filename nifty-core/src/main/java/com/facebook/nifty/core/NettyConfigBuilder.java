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

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.ServerSocketChannelConfig;
import io.netty.channel.socket.SocketChannelConfig;

import java.lang.reflect.Proxy;
import java.util.Map;

/*
 * Hooks for configuring various parts of Netty.
 */
public class NettyConfigBuilder extends NettyConfigBuilderBase
{
    private final SocketChannelConfig socketChannelConfig = (SocketChannelConfig) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[]{SocketChannelConfig.class},
            new Magic());
    private final ServerSocketChannelConfig serverSocketChannelConfig = (ServerSocketChannelConfig) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[]{ServerSocketChannelConfig.class},
            new Magic());

    @Inject
    public NettyConfigBuilder()
    {
        // configuration defaults
        socketChannelConfig.setAllocator(PooledByteBufAllocator.DEFAULT);
        socketChannelConfig.setDefaultHandlerByteBufType(ChannelConfig.ChannelHandlerByteBufType
                                                                 .HEAP);
    }

    public SocketChannelConfig getSocketChannelConfig()
    {
        return socketChannelConfig;
    }

    public ServerSocketChannelConfig getServerSocketChannelConfig()
    {
        return serverSocketChannelConfig;
    }

    public void applyConfig(ServerBootstrap bootstrap)
    {
        for (Map.Entry<ChannelOption<?>, Object> entry : getServerSocketChannelConfig().getOptions().entrySet()) {
            bootstrap.option((ChannelOption<Object>) entry.getKey(), entry.getValue());
        }

        for (Map.Entry<ChannelOption<?>, Object> entry : getSocketChannelConfig().getOptions().entrySet()) {
            bootstrap.childOption((ChannelOption<Object>) entry.getKey(), entry.getValue());
        }
    }
}
