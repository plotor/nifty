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

import com.facebook.nifty.core.NettyConfigBuilderBase;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannelConfig;

import java.lang.reflect.Proxy;
import java.util.Map;

/*
 * Hooks for configuring various parts of Netty.
 */
public class NettyClientConfigBuilder extends NettyConfigBuilderBase
{
    // The constants come directly from Netty but are private in Netty. We need these default
    // values to call the NioClientSocketChannelFactory constructor with a custom timer.
    private static final int DEFAULT_BOSS_THREAD_COUNT = 1;
    private static final int DEFAULT_WORKER_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private int workerThreadCount = DEFAULT_WORKER_THREAD_COUNT;
    private String name = "";

    private final SocketChannelConfig socketChannelConfig = (SocketChannelConfig) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[]{SocketChannelConfig.class},
            new Magic()
    );

    @Inject
    public NettyClientConfigBuilder()
    {
    }

    public SocketChannelConfig getSocketChannelConfig()
    {
        return socketChannelConfig;
    }

    public NettyClientConfigBuilder setNiftyWorkerThreadCount(int workerThreadCount)
    {
        this.workerThreadCount = workerThreadCount;
        return this;
    }

    public int getNiftyWorkerThreadCount()
    {
        return workerThreadCount;
    }

    public NettyClientConfigBuilder setNiftyName(String name)
    {
        Preconditions.checkNotNull(name, "name is null");
        this.name = name;
        return this;
    }

    public String getNiftyName()
    {
        return name;
    }

    public void applyConfig(Bootstrap bootstrap)
    {
        for (Map.Entry<ChannelOption<?>, Object> entry : getSocketChannelConfig().getOptions().entrySet()) {
            bootstrap.option((ChannelOption<Object>) entry.getKey(), entry.getValue());
        }
    }
}
