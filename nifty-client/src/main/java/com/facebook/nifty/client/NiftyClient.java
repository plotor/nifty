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

import com.google.common.base.Strings;

import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;

import com.facebook.nifty.client.socks.SocksClientBootstrapWrapper;
import com.facebook.nifty.core.ShutdownUtil;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.units.Duration;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.thrift.transport.TTransportException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.HashedWheelTimer;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class NiftyClient implements Closeable
{
    public static final Duration DEFAULT_CONNECT_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    public static final Duration DEFAULT_READ_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    private static final Duration DEFAULT_WRITE_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    private static final int DEFAULT_MAX_FRAME_SIZE = 16777216;

    private final NettyClientConfigBuilder configBuilder;
    private final InetSocketAddress defaultSocksProxyAddress;
    private final ChannelGroup allChannels = new DefaultChannelGroup();
    private final HashedWheelTimer hashedWheelTimer;
    private final NioEventLoopGroup ioWorkerEventLoopGroup;

    /**
     * Creates a new NiftyClient with defaults: cachedThreadPool for bossExecutor and workerExecutor
     */
    public NiftyClient()
    {
        this(new NettyClientConfigBuilder());
    }

    public NiftyClient(NettyClientConfigBuilder configBuilder)
    {
        this(configBuilder, null);
    }

    public NiftyClient(
            NettyClientConfigBuilder configBuilder,
            @Nullable InetSocketAddress defaultSocksProxyAddress)
    {
        this.configBuilder = configBuilder;

        String name = configBuilder.getNiftyName();
        String prefix = "nifty-client" + (Strings.isNullOrEmpty(name) ? "" : "-" + name);

        ThreadFactory timerThreadFactory = renamingDaemonThreadFactory(prefix + "-timer-%s");
        ThreadFactory ioWorkerThreadFactory = renamingDaemonThreadFactory(prefix + "-worker-%s");
        int workerThreadCount = configBuilder.getNiftyWorkerThreadCount();

        this.defaultSocksProxyAddress = defaultSocksProxyAddress;

        this.hashedWheelTimer = new HashedWheelTimer(timerThreadFactory);
        this.ioWorkerEventLoopGroup = new NioEventLoopGroup(workerThreadCount, ioWorkerThreadFactory);
    }

    public <T extends NiftyClientChannel> ListenableFuture<T> connectAsync(
            NiftyClientConnector<T> clientChannelConnector)
    {
        return connectAsync(clientChannelConnector,
                            DEFAULT_CONNECT_TIMEOUT,
                            DEFAULT_READ_TIMEOUT,
                            DEFAULT_WRITE_TIMEOUT,
                            DEFAULT_MAX_FRAME_SIZE,
                            defaultSocksProxyAddress);
    }

    public <T extends NiftyClientChannel> ListenableFuture<T> connectAsync(
            NiftyClientConnector<T> clientChannelConnector,
            Duration connectTimeout,
            Duration receiveTimeout,
            Duration sendTimeout,
            int maxFrameSize)
    {
        return connectAsync(clientChannelConnector,
                            connectTimeout,
                            receiveTimeout,
                            sendTimeout,
                            maxFrameSize,
                            defaultSocksProxyAddress);
    }

    public <T extends NiftyClientChannel> ListenableFuture<T> connectAsync(
            NiftyClientConnector<T> clientChannelConnector,
            Duration connectTimeout,
            Duration receiveTimeout,
            Duration sendTimeout,
            int maxFrameSize,
            @Nullable InetSocketAddress socksProxyAddress)
    {
        BootstrapWrapper bootstrap = createClientBootstrap(configBuilder, socksProxyAddress);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis());
        bootstrap.handler(clientChannelConnector.newChannelInitializer(maxFrameSize));

        ChannelFuture nettyChannelFuture = clientChannelConnector.connect(bootstrap);
        nettyChannelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Channel channel = future.channel();
                if (channel != null && channel.isOpen()) {
                    // Add the channel to allChannels, and set it up to be removed when closed
                    allChannels.add(channel);
                    channel.closeFuture().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            Channel channel = future.channel();
                            allChannels.remove(channel);
                        }
                    });
                }
            }
        });
        return new TNiftyFuture<>(clientChannelConnector,
                                  receiveTimeout,
                                  sendTimeout,
                                  nettyChannelFuture);
    }

    // trying to mirror the synchronous nature of TSocket as much as possible here.
    public TNiftyClientTransport connectSync(InetSocketAddress addr)
            throws TTransportException, InterruptedException
    {
        return connectSync(addr, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_WRITE_TIMEOUT, DEFAULT_MAX_FRAME_SIZE);
    }

    public TNiftyClientTransport connectSync(
            InetSocketAddress addr,
            Duration connectTimeout,
            Duration receiveTimeout,
            Duration sendTimeout,
            int maxFrameSize)
            throws TTransportException, InterruptedException
    {
        return connectSync(addr, connectTimeout, receiveTimeout, sendTimeout, maxFrameSize, defaultSocksProxyAddress);
    }

    public TNiftyClientTransport connectSync(
            InetSocketAddress addr,
            Duration connectTimeout,
            Duration receiveTimeout,
            Duration sendTimeout,
            int maxFrameSize,
            @Nullable InetSocketAddress socksProxyAddress)
            throws TTransportException, InterruptedException
    {
        // TODO: implement send timeout for sync client
        BootstrapWrapper bootstrap = createClientBootstrap(configBuilder, socksProxyAddress);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis());
        bootstrap.handler(new SyncClientChannelInitializer(maxFrameSize));

        ChannelFuture f = bootstrap.connect(addr);
        f.await();
        Channel channel = f.channel();
        if (f.cause() != null) {
            String message = String.format("unable to connect to %s:%d %s",
                    addr.getHostName(),
                    addr.getPort(),
                    socksProxyAddress == null ? "" : "via socks proxy at " + socksProxyAddress);
            throw new TTransportException(message, f.cause());
        }

        if (f.isSuccess() && (channel != null)) {
            if (channel.isOpen()) {
                // Add the channel to allChannels, and set it up to be removed when closed
                allChannels.add(channel);
                channel.closeFuture().addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        Channel channel = future.channel();
                        allChannels.remove(channel);
                    }
                });
            }

            TNiftyClientTransport transport = new TNiftyClientTransport(channel, receiveTimeout);
            channel.pipeline().addLast("thrift", transport);
            return transport;
        }

        throw new TTransportException(String.format(
                "unknown error connecting to %s:%d %s",
                addr.getHostName(),
                addr.getPort(),
                socksProxyAddress == null ? "" : "via socks proxy at " + socksProxyAddress
        ));
    }

    @Override
    public void close()
    {
        // Stop the timer thread first, so no timeouts can fire during the rest of the
        // shutdown process
        hashedWheelTimer.stop();

        ShutdownUtil.closeChannels(allChannels);
        ShutdownUtil.shutdownEventLoopGroup(ioWorkerEventLoopGroup, "ioWorkerEventLoopGroup");
    }

    private ThreadFactory renamingDaemonThreadFactory(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
    }

    private BootstrapWrapper createClientBootstrap(NettyClientConfigBuilder clientConfigBuilder,
                                                   InetSocketAddress socksProxyAddress)
    {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(ioWorkerEventLoopGroup)
                 .channel(NioSocketChannel.class);
        configBuilder.applyConfig(bootstrap);

        BootstrapWrapper wrapper;
        if (socksProxyAddress != null) {
            wrapper = new SocksClientBootstrapWrapper(socksProxyAddress, bootstrap);
        }
        else {
            wrapper= new BootstrapWrapper(bootstrap);
        }

        return wrapper;
    }

    private class TNiftyFuture<T extends NiftyClientChannel> extends AbstractFuture<T>
    {
        private TNiftyFuture(final NiftyClientConnector<T> clientChannelConnector,
                             final Duration receiveTimeout,
                             final Duration sendTimeout,
                             final ChannelFuture channelFuture)
        {
            channelFuture.addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception
                {
                    if (future.isSuccess()) {
                        Channel nettyChannel = future.channel();
                        T channel = clientChannelConnector.newThriftClientChannel(nettyChannel,
                                                                                  hashedWheelTimer);
                        channel.setReceiveTimeout(receiveTimeout);
                        channel.setSendTimeout(sendTimeout);
                        set(channel);
                    }
                    else if (future.isCancelled()) {
                        cancel(true);
                    }
                    else {
                        setException(future.cause());
                    }
                }
            });
        }
    }

}
