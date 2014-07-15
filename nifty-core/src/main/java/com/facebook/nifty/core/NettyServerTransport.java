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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.nifty.core.NiftyChannelGroups.CHANNEL_GROUP_EVENT_EXECUTOR;

/**
 * A core channel the decode framed Thrift message, dispatches to the TProcessor given
 * and then encode message back to Thrift frame.
 */
public class NettyServerTransport
{
    private static final Logger log = LoggerFactory.getLogger(NettyServerTransport.class);

    private final int port;
    private static final int NO_WRITER_IDLE_TIMEOUT = 0;
    private static final int NO_ALL_IDLE_TIMEOUT = 0;
    private ServerBootstrap bootstrap;
    private final ChannelGroup allChannels;
    private Channel serverChannel;
    private final ThriftServerDef def;
    private final NettyServerConfig nettyServerConfig;
    private final ChannelStatistics channelStatistics;

    public NettyServerTransport(final ThriftServerDef def)
    {
        this(def, NettyServerConfig.newBuilder().build(), new DefaultChannelGroup(CHANNEL_GROUP_EVENT_EXECUTOR));
    }

    @Inject
    public NettyServerTransport(
            final ThriftServerDef def,
            final NettyServerConfig nettyServerConfig,
            final ChannelGroup allChannels)
    {
        this.def = def;
        this.nettyServerConfig = nettyServerConfig;
        this.port = def.getServerPort();
        this.allChannels = allChannels;
        this.channelStatistics = new ChannelStatistics(allChannels);
    }

    public void start() throws InterruptedException
    {
        start(new ServerBootstrap().group(new NioEventLoopGroup(1, new DefaultThreadFactory("nifty-server-acceptor-pool")),
                                          new NioEventLoopGroup(0, new DefaultThreadFactory("nifty-server-ioworker-pool"))));
    }

    public void start(EventLoopGroup bossGroup, EventLoopGroup ioWorkerGroup) throws InterruptedException
    {
        start(new ServerBootstrap().group(bossGroup, ioWorkerGroup));
    }

    public void start(ServerBootstrap serverBootstrap) throws InterruptedException
    {
        // connectionLimiter must be instantiated exactly once (and thus outside the pipeline factory)
        final ConnectionLimiter connectionLimiter = new ConnectionLimiter(def.getMaxConnections());

        this.bootstrap = serverBootstrap;

        nettyServerConfig.getBuilder().applyConfig(bootstrap);

        bootstrap.channel(NioServerSocketChannel.class);

        bootstrap.childHandler(new ChannelInitializer<NioSocketChannel>() {
            @Override
            protected void initChannel(NioSocketChannel channel) throws Exception
            {
                ChannelPipeline cp = channel.pipeline();
                TProtocolFactory inputProtocolFactory = def.getDuplexProtocolFactory().getInputProtocolFactory();
                // TODO: re-nable this
                //NiftySecurityHandlers securityHandlers = def.getSecurityFactory().getSecurityHandlers(def, nettyServerConfig);
                cp.addLast("connectionContext", new ConnectionContextHandler());
                cp.addLast("connectionLimiter", connectionLimiter);
                cp.addLast(ChannelStatistics.NAME, channelStatistics);
                // TODO(NETTY4): make auth & encryption work again
                //cp.addLast("encryptionHandler", securityHandlers.getEncryptionHandler());
                cp.addLast("frameCodec", def.getThriftFrameCodecFactory().create(def.getMaxFrameSize(),
                                                                                 inputProtocolFactory));
                if (def.getClientIdleTimeout() != null) {
                    // Add handlers to detect idle client connections and disconnect them
                    cp.addLast("idleDisconnectHandler", new IdleStateHandler(def.getClientIdleTimeout().toMillis(),
                                                                             NO_WRITER_IDLE_TIMEOUT,
                                                                             NO_ALL_IDLE_TIMEOUT,
                                                                             TimeUnit.MILLISECONDS) {
                        @Override
                        protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception
                        {
                            ctx.channel().close();
                        }
                    });
                }

                //cp.addLast("authHandler", securityHandlers.getAuthenticationHandler());
                cp.addLast("dispatcher", new NiftyDispatcher(def, nettyServerConfig.getTimer()));
                cp.addLast("exceptionLogger", new NiftyExceptionLogger());
            }
        });

        serverChannel = bootstrap.bind(new InetSocketAddress(port)).sync().channel();
        SocketAddress actualSocket = serverChannel.localAddress();
        if (actualSocket instanceof InetSocketAddress) {
            int actualPort = ((InetSocketAddress) actualSocket).getPort();
            log.info("started transport {}:{} (:{})", def.getName(), actualPort, port);
        }
        else {
            log.info("started transport {}:{}", def.getName(), port);
        }
    }

    public void stop()
            throws InterruptedException
    {
        if (serverChannel != null) {
            log.info("stopping transport {}:{}", def.getName(), port);
            // first stop accepting
            final CountDownLatch latch = new CountDownLatch(1);
            serverChannel.close().addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception
                {
                    // stop and process remaining in-flight invocations
                    if (def.getExecutor() instanceof ExecutorService) {
                        ExecutorService exe = (ExecutorService) def.getExecutor();
                        ShutdownUtil.shutdownExecutor(exe, "dispatcher");
                    }
                    latch.countDown();
                }
            });
            latch.await();
            serverChannel = null;
        }

        // If the channelFactory was created by us, we should also clean it up. If the
        // channelFactory was passed in by NiftyBootstrap, then it may be shared so don't clean
        // it up.
        ShutdownUtil.shutdownChannelFactory(bootstrap, allChannels);
    }

    public Channel getServerChannel()
    {
        return serverChannel;
    }

    @ChannelHandler.Sharable
    private static class ConnectionLimiter extends ChannelInboundHandlerAdapter
    {
        private final AtomicInteger numConnections;
        private final int maxConnections;

        public ConnectionLimiter(int maxConnections)
        {
            this.maxConnections = maxConnections;
            this.numConnections = new AtomicInteger(0);
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception
        {
            if (maxConnections > 0) {
                if (numConnections.incrementAndGet() > maxConnections) {
                    ctx.channel().close();
                    // numConnections will be decremented in channelClosed
                    log.info("Accepted connection above limit ({}). Dropping.", maxConnections);
                }
            }

            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception
        {
            if (maxConnections > 0) {
                if (numConnections.decrementAndGet() < 0) {
                    log.error("BUG in ConnectionLimiter");
                }
            }

            super.channelUnregistered(ctx);
        }
    }

    public NiftyMetrics getMetrics()
    {
        return channelStatistics;
    }
}
