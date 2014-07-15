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

import com.facebook.nifty.core.NiftyChannelInitializer;
import com.facebook.nifty.core.NiftyChannelInitializers;
import com.facebook.nifty.core.ShutdownUtil;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import io.airlift.units.Duration;
import io.netty.bootstrap.ChannelFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.Timer;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.nifty.core.NiftyChannelGroups.CHANNEL_GROUP_EVENT_EXECUTOR;
import static com.facebook.nifty.core.NiftyChannelInitializers.composeChannelInitializers;
import static com.google.common.base.Preconditions.checkNotNull;

public class NiftyClient implements Closeable
{
    public static final Duration DEFAULT_CONNECT_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    public static final Duration DEFAULT_RECEIVE_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    private static final Duration DEFAULT_SEND_TIMEOUT = new Duration(2, TimeUnit.SECONDS);

    private static final int DEFAULT_MAX_FRAME_SIZE = 16777216;

    private final NettyClientConfig nettyClientConfig;
    //private final ExecutorService bossExecutor;
    //private final ExecutorService workerExecutor;
    private final HostAndPort defaultSocksProxyAddress;
    private final ChannelGroup allChannels = new DefaultChannelGroup(CHANNEL_GROUP_EVENT_EXECUTOR);
    private final Timer timer;
    private final NioEventLoopGroup workerGroup;
    private final ChannelFactory channelFactory = null;

    /**
     * Creates a new NiftyClient with defaults: cachedThreadPool for bossExecutor and workerExecutor
     */
    public NiftyClient()
    {
        this(NettyClientConfig.newBuilder().build());
    }

    public NiftyClient(NettyClientConfig nettyClientConfig)
    {
        this.nettyClientConfig = nettyClientConfig;

        this.timer = nettyClientConfig.getTimer();
        //this.bossExecutor = nettyClientConfig.getBossExecutor();
        //this.workerExecutor = nettyClientConfig.getWorkerExecutor();
        this.defaultSocksProxyAddress = nettyClientConfig.getDefaultSocksProxyAddress();

        //int bossThreadCount = nettyClientConfig.getBossThreadCount();
        int workerThreadCount = nettyClientConfig.getWorkerThreadCount();

        //NioWorkerPool workerPool = new NioWorkerPool(workerExecutor, workerThreadCount, ThreadNameDeterminer.CURRENT);
        //NioClientBossPool bossPool = new NioClientBossPool(bossExecutor, bossThreadCount, timer, ThreadNameDeterminer.CURRENT);

        //this.channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        // TODO(NETTY4): do i need to fix thread names?
        this.workerGroup = new NioEventLoopGroup(workerThreadCount);
    }

    public <T extends NiftyClientChannel> ListenableFuture<T> connectAsync(
            NiftyClientConnector<T> clientChannelConnector)
    {
        return connectAsync(clientChannelConnector,
                            DEFAULT_CONNECT_TIMEOUT,
                            DEFAULT_RECEIVE_TIMEOUT,
                            DEFAULT_SEND_TIMEOUT,
                            DEFAULT_MAX_FRAME_SIZE,
                            defaultSocksProxyAddress);
    }

    public HostAndPort getDefaultSocksProxyAddress()
    {
        return defaultSocksProxyAddress;
    }

    public <T extends NiftyClientChannel> ListenableFuture<T> connectAsync(
        NiftyClientConnector<T> clientChannelConnector,
        @Nullable Duration connectTimeout,
        @Nullable Duration receiveTimeout,
        @Nullable Duration sendTimeout,
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
            @Nullable Duration connectTimeout,
            @Nullable Duration receiveTimeout,
            @Nullable Duration sendTimeout,
            int maxFrameSize,
            @Nullable HostAndPort socksProxyAddress)
    {
        checkNotNull(clientChannelConnector, "clientChannelConnector is null");

        ClientBootstrap bootstrap = createClientBootstrap(socksProxyAddress);
        bootstrap.setOptions(nettyClientConfig.getBootstrapOptions());

        if (connectTimeout != null) {
            bootstrap.setOption("connectTimeoutMillis", (int) connectTimeout.toMillis());
        }

        ChannelHandler handler = composeChannelInitializers(
                clientChannelConnector.newChannelInitializer(maxFrameSize, nettyClientConfig),
                NiftyChannelInitializers.getOptionsInitializer(nettyClientConfig.getBootstrapOptions()));
        bootstrap.handler(handler);

        ChannelFuture nettyChannelFuture = clientChannelConnector.connect(bootstrap);
        nettyChannelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Channel channel = future.channel();
                if (future.isSuccess() && channel.isActive()) {
                    allChannels.add(channel);
                }
            }
        });
        return new TNiftyFuture<>(clientChannelConnector,
                                  receiveTimeout,
                                  sendTimeout,
                                  nettyChannelFuture);
    }

    public <T extends NiftyClientChannel> TNiftyClientChannelTransport connectSync(
            Class<? extends TServiceClient> clientClass,
            NiftyClientConnector<T> clientChannelConnector)
            throws TTransportException, InterruptedException
    {
        return connectSync(
                clientClass,
                clientChannelConnector,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_RECEIVE_TIMEOUT,
                DEFAULT_SEND_TIMEOUT,
                DEFAULT_MAX_FRAME_SIZE,
                defaultSocksProxyAddress);
    }

    public <T extends NiftyClientChannel> TNiftyClientChannelTransport connectSync(
            Class<? extends TServiceClient> clientClass,
            NiftyClientConnector<T> clientChannelConnector,
            @Nullable Duration connectTimeout,
            @Nullable Duration receiveTimeout,
            @Nullable Duration sendTimeout,
            int maxFrameSize)
            throws TTransportException, InterruptedException
    {
        return connectSync(
                clientClass,
                clientChannelConnector,
                connectTimeout,
                receiveTimeout,
                sendTimeout,
                maxFrameSize,
                null);
    }

    public <T extends NiftyClientChannel> TNiftyClientChannelTransport connectSync(
            Class<? extends TServiceClient> clientClass,
            NiftyClientConnector<T> clientChannelConnector,
            @Nullable Duration connectTimeout,
            @Nullable Duration receiveTimeout,
            @Nullable Duration sendTimeout,
            int maxFrameSize,
            @Nullable HostAndPort socksProxyAddress)
            throws TTransportException, InterruptedException
    {
        try {
            T channel =
                    connectAsync(
                            clientChannelConnector,
                            connectTimeout,
                            receiveTimeout,
                            sendTimeout,
                            maxFrameSize,
                            socksProxyAddress).get();
            return new TNiftyClientChannelTransport(clientClass, channel);
        }
        catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), TTransportException.class);
            throw new TTransportException(TTransportException.UNKNOWN, "Failed to establish client connection", e);
        }
    }

    /**
     * @deprecated Use the versions of connectSync that take a client {@link Class} and a
     * {@link com.facebook.nifty.client.NiftyClientConnector} instead (this method acts like such a
     * transport around a {@link com.facebook.nifty.client.FramedClientConnector}
     */
    @Deprecated
    public TNiftyClientTransport connectSync(InetSocketAddress addr)
            throws TTransportException, InterruptedException
    {
        return connectSync(addr, DEFAULT_CONNECT_TIMEOUT, DEFAULT_RECEIVE_TIMEOUT, DEFAULT_SEND_TIMEOUT, DEFAULT_MAX_FRAME_SIZE);
    }

    /**
     * @deprecated Use the versions of connectSync that take a client {@link Class} and a
     * {@link com.facebook.nifty.client.NiftyClientConnector} instead (this method acts like such a
     * transport around a {@link com.facebook.nifty.client.FramedClientConnector}
     */
    @Deprecated
    public TNiftyClientTransport connectSync(
            InetSocketAddress addr,
            @Nullable Duration connectTimeout,
            @Nullable Duration receiveTimeout,
            @Nullable Duration sendTimeout,
            int maxFrameSize)
            throws TTransportException, InterruptedException
    {
        return connectSync(addr, connectTimeout, receiveTimeout, sendTimeout, maxFrameSize, defaultSocksProxyAddress);
    }

    /**
     * @deprecated Use the versions of connectSync that take a client {@link Class} and a
     * {@link com.facebook.nifty.client.NiftyClientConnector} instead (this method acts like such a
     * transport around a {@link com.facebook.nifty.client.FramedClientConnector}
     */
    @Deprecated
    public TNiftyClientTransport connectSync(
            InetSocketAddress addr,
            @Nullable Duration connectTimeout,
            @Nullable Duration receiveTimeout,
            @Nullable Duration sendTimeout,
            int maxFrameSize,
            @Nullable HostAndPort socksProxyAddress)
            throws TTransportException, InterruptedException
    {
        // TODO: implement send timeout for sync client
        ClientBootstrap bootstrap = createClientBootstrap(socksProxyAddress);

        NiftyChannelInitializer<Channel> channelInitializer =
                NiftyChannelInitializers.composeChannelInitializers(
                        new DefaultNiftyClientChannelInitializer<>(maxFrameSize),
                        NiftyChannelInitializers.getOptionsInitializer(nettyClientConfig.getBootstrapOptions()));
        bootstrap.handler(channelInitializer);

        if (connectTimeout != null) {
            bootstrap.setOption("connectTimeoutMillis", (int) connectTimeout.toMillis());
        }

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

        if (f.isSuccess() && channel != null) {
            if (channel.isOpen()) {
                allChannels.add(channel);
            }

            TNiftyClientTransport transport = new TNiftyClientTransport(channel, receiveTimeout);
            channel.pipeline().addLast("thrift", transport.getChannelHandler());
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
        timer.stop();

        ShutdownUtil.shutdownChannelFactory(channelFactory,
                                            null,
                                            workerGroup,
                                            allChannels);
    }

    private ClientBootstrap createClientBootstrap(@Nullable HostAndPort socksProxyAddress)
    {
        //if (socksProxyAddress != null) {
        //    return new Socks4ClientBootstrap(channelFactory, toInetAddress(socksProxyAddress));
        //}
        //else {
            return new DefaultClientBootstrap().group(workerGroup);
        //}
    }

    private static InetSocketAddress toInetAddress(HostAndPort hostAndPort)
    {
        return (hostAndPort == null) ? null : new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort());
    }

    private class TNiftyFuture<T extends NiftyClientChannel> extends AbstractFuture<T>
    {
        private TNiftyFuture(final NiftyClientConnector<T> clientChannelConnector,
                             @Nullable final Duration receiveTimeout,
                             @Nullable final Duration sendTimeout,
                             final ChannelFuture channelFuture)
        {
            channelFuture.addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception
                {
                    try {
                        if (future.isSuccess()) {
                            Channel nettyChannel = future.channel();
                            T channel = clientChannelConnector.newThriftClientChannel(nettyChannel,
                                                                                      nettyClientConfig);
                            channel.setReceiveTimeout(receiveTimeout);
                            channel.setSendTimeout(sendTimeout);
                            set(channel);
                        }
                        else if (future.isCancelled()) {
                            if (!cancel(true)) {
                                setException(new TTransportException("Unable to cancel client channel connection"));
                            }
                        }
                        else {
                            throw future.cause();
                        }
                    }
                    catch (Throwable t) {
                        setException(new TTransportException("Failed to connect client channel", t));
                    }
                }
            });
        }
    }
}
