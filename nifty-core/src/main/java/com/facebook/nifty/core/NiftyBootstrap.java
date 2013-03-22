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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

/**
 * A lifecycle object that manages starting up and shutting down multiple core channels.
 */
public class NiftyBootstrap
{
    public static final int DEFAULT_IO_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private final ArrayList<NettyServerTransport> transports;

    /**
     * This takes a Set of ThriftServerDef. Use Guice Multibinder to inject.
     */
    @Inject
    public NiftyBootstrap(
            Set<ThriftServerDef> thriftServerDefs,
            NettyConfigBuilder configBuilder,
            ChannelGroup allChannels)
    {
        this.transports = new ArrayList<>();
        for (ThriftServerDef thriftServerDef : thriftServerDefs) {
            transports.add(new NettyServerTransport(thriftServerDef,
                                                    configBuilder,
                                                    allChannels));
        }

    }

    @PostConstruct
    public void start() throws InterruptedException
    {
        ThreadFactory parentThreadFactory = renamingThreadFactory("nifty-acceptor-%s");
        ThreadFactory childThreadFactory = renamingThreadFactory("nifty-io-%s");

        EventLoopGroup parentGroup = new NioEventLoopGroup(1, parentThreadFactory);
        EventLoopGroup childGroup = new NioEventLoopGroup(DEFAULT_IO_THREAD_COUNT, childThreadFactory);
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(parentGroup, childGroup);
        serverBootstrap.channel(NioServerSocketChannel.class);

        for (NettyServerTransport transport : transports) {
            transport.start(serverBootstrap);
        }
    }

    @PreDestroy
    public void stop() throws InterruptedException
    {
        for (NettyServerTransport transport : transports) {
            transport.stop();
        }
    }

    private ThreadFactory renamingThreadFactory(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }
}
