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
import io.netty.bootstrap.ChannelFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.AttributeKey;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class DelegatingClientBootstrap implements ClientBootstrap
{
    private final Bootstrap bootstrap;

    public DelegatingClientBootstrap(Bootstrap bootstrap)
    {
        this.bootstrap = bootstrap;
    }

    public Bootstrap remoteAddress(SocketAddress remoteAddress)
    {
        return bootstrap.remoteAddress(remoteAddress);
    }

    public Bootstrap remoteAddress(InetAddress inetHost, int inetPort)
    {
        return bootstrap.remoteAddress(inetHost, inetPort);
    }

    public <T> Bootstrap option(ChannelOption<T> option, T value)
    {
        return bootstrap.option(option, value);
    }

    public ChannelFuture connect()
    {
        return bootstrap.connect();
    }

    public ChannelFuture connect(SocketAddress remoteAddress)
    {
        return bootstrap.connect(remoteAddress);
    }

    public ChannelFuture connect(InetAddress inetHost, int inetPort)
    {
        return connect(new InetSocketAddress(inetHost, inetPort));
    }

    public ChannelFuture connect(String inetHost, int inetPort)
    {
        return connect(new InetSocketAddress(inetHost, inetPort));
    }

    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress)
    {
        return bootstrap.connect(remoteAddress, localAddress);
    }

    public ChannelFuture bind(SocketAddress localAddress)
    {
        return bootstrap.bind(localAddress);
    }

    public Bootstrap channel(Class<? extends Channel> channelClass)
    {
        return bootstrap.channel(channelClass);
    }

    public Bootstrap channelFactory(ChannelFactory<? extends Channel> channelFactory)
    {
        return bootstrap.channelFactory(channelFactory);
    }

    public Bootstrap localAddress(SocketAddress localAddress)
    {
        return bootstrap.localAddress(localAddress);
    }

    public Bootstrap localAddress(int inetPort)
    {
        return localAddress(new InetSocketAddress(inetPort));
    }

    public Bootstrap localAddress(String inetHost, int inetPort)
    {
        return localAddress(new InetSocketAddress(inetHost, inetPort));
    }

    public Bootstrap localAddress(InetAddress inetHost, int inetPort)
    {
        return localAddress(new InetSocketAddress(inetHost, inetPort));
    }

    public Bootstrap handler(ChannelHandler handler)
    {
        return bootstrap.handler(handler);
    }

    public Bootstrap validate()
    {
        return bootstrap.validate();
    }

    public ChannelFuture register()
    {
        return bootstrap.register();
    }

    public ChannelFuture bind(int inetPort)
    {
        return bootstrap.bind(inetPort);
    }

    public ChannelFuture bind(String inetHost, int inetPort)
    {
        return bootstrap.bind(inetHost, inetPort);
    }

    public <T> Bootstrap attr(AttributeKey<T> key, T value)
    {
        return bootstrap.attr(key, value);
    }

    public EventLoopGroup group()
    {
        return bootstrap.group();
    }

    public ChannelFuture bind(InetAddress inetHost, int inetPort)
    {
        return bootstrap.bind(inetHost, inetPort);
    }

    public ChannelFuture bind()
    {
        return bootstrap.bind();
    }

    public Bootstrap group(EventLoopGroup group)
    {
        return bootstrap.group(group);
    }

    public Bootstrap remoteAddress(String inetHost, int inetPort)
    {
        return bootstrap.remoteAddress(inetHost, inetPort);
    }
}
