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
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * A core channel the decode framed Thrift message, dispatches to the TProcessor given
 * and then encode message back to Thrift frame.
 */
public class NettyServerTransport
{
    private static final Logger log = LoggerFactory.getLogger(NettyServerTransport.class);

    private final int port;
    private Channel serverChannel;
    private final ThriftServerDef def;
    private final NettyConfigBuilder configBuilder;
    private final ChannelGroup allChannels;

    @Inject
    public NettyServerTransport(
            final ThriftServerDef def,
            NettyConfigBuilder configBuilder,
            final ChannelGroup allChannels)
    {
        this.def = def;
        this.allChannels = allChannels;
        this.configBuilder = configBuilder;
        this.port = def.getServerPort();
        if (def.isHeaderTransport()) {
            throw new UnsupportedOperationException("ASF version does not support THeaderTransport !");
        }
    }

    public void start(ServerBootstrap bootstrap) throws InterruptedException
    {
        configBuilder.applyConfig(bootstrap);

        bootstrap.childHandler(new ChannelInitializer<NioSocketChannel>()
        {
            @Override
            protected void initChannel(NioSocketChannel ch) throws Exception
            {
                // Setup channel pipeline

                ChannelPipeline cp = ch.pipeline();

                cp.addLast(ChannelStatistics.NAME, new ChannelStatistics(allChannels));
                cp.addLast("frameDecoder", new ThriftFrameDecoder(def.getMaxFrameSize(),
                                                                  def.getInProtocolFactory()));
                if (def.getClientIdleTimeout() != null) {
                    // Add handler to detect idle client connections and disconnect them
                    cp.addLast("idleDisconnectHandler", new IdleClientDisconnectHandler(def.getClientIdleTimeout()));
                }
                cp.addLast("dispatcher", new NiftyDispatcher(def));
            }
        });

        log.info("starting transport {}:{}", def.getName(), port);

        serverChannel = bootstrap.bind(new InetSocketAddress(port)).sync().channel();
    }

    public void stop()
            throws InterruptedException
    {
        if (serverChannel != null) {
            log.info("stopping transport {}:{}", def.getName(), port);

            // first stop accepting
            serverChannel.eventLoop().shutdown();

            ShutdownUtil.closeChannels(allChannels);

            // stop and process remaining in-flight invocations
            if (def.getExecutor() instanceof ExecutorService) {
                ExecutorService exe = (ExecutorService) def.getExecutor();
                ShutdownUtil.shutdownExecutor(exe, "dispatcher");
            }

            serverChannel = null;
        }
    }

    public Channel getServerChannel() {
        return serverChannel;
    }
}
