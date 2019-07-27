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

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.socket.ServerSocketChannelConfig;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.util.Timer;
import static java.util.concurrent.Executors.newCachedThreadPool;

import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Hooks for configuring various parts of Netty.
 */
public class NettyServerConfigBuilder extends NettyConfigBuilderBase<NettyServerConfigBuilder> {

    private final SocketChannelConfig socketChannelConfig = (SocketChannelConfig) Proxy.newProxyInstance(
            this.getClass().getClassLoader(),
            new Class<?>[] {SocketChannelConfig.class},
            new Magic("child.")
    );

    private final ServerSocketChannelConfig serverSocketChannelConfig = (ServerSocketChannelConfig) Proxy.newProxyInstance(
            this.getClass().getClassLoader(),
            new Class<?>[] {ServerSocketChannelConfig.class},
            new Magic(""));

    @Inject
    public NettyServerConfigBuilder() {
        // Thrift turns TCP_NODELAY by default, and turning it off can have latency implications
        // so let's turn it on by default as well. It can still be switched off by explicitly
        // calling setTcpNodelay(false) after construction.
        this.getSocketChannelConfig().setTcpNoDelay(true);
    }

    /**
     * Returns an implementation of {@link io.netty.channel.socket.SocketChannelConfig} which will
     * be applied to all {@link io.netty.channel.socket.nio.NioSocketChannel} instances created to
     * manage connections accepted by the server.
     *
     * @return A mutable {@link io.netty.channel.socket.SocketChannelConfig}
     */
    public SocketChannelConfig getSocketChannelConfig() {
        return socketChannelConfig;
    }

    /**
     * Returns an implementation of {@link ServerSocketChannelConfig}
     * which will be applied to the {@link java.nio.channels.ServerSocketChannel}
     * the server will use to accept connections.
     *
     * @return A mutable {@link ServerSocketChannelConfig}
     */
    public ServerSocketChannelConfig getServerSocketChannelConfig() {
        return serverSocketChannelConfig;
    }

    public NettyServerConfig build() {
        Timer timer = this.getTimer();
        ExecutorService bossExecutor = this.getBossExecutor();
        int bossThreadCount = this.getBossThreadCount();
        ExecutorService workerExecutor = this.getWorkerExecutor();
        int workerThreadCount = this.getWorkerThreadCount();

        return new NettyServerConfig(
                this,
                this.getBootstrapOptions(),
                timer != null ? timer : new NiftyTimer(this.threadNamePattern("")),
                bossExecutor != null ? bossExecutor : this.buildDefaultBossExecutor(),
                bossThreadCount,
                workerExecutor != null ? workerExecutor : this.buildDefaultWorkerExecutor(),
                workerThreadCount
        );
    }

    private ExecutorService buildDefaultBossExecutor() {
        return newCachedThreadPool(this.renamingThreadFactory(this.threadNamePattern("-boss-%s")));
    }

    private ExecutorService buildDefaultWorkerExecutor() {
        return newCachedThreadPool(this.renamingThreadFactory(this.threadNamePattern("-worker-%s")));
    }

    private String threadNamePattern(String suffix) {
        String niftyName = this.getNiftyName();
        return "nifty-server" + (Strings.isNullOrEmpty(niftyName) ? "" : "-" + niftyName) + suffix;
    }

    private ThreadFactory renamingThreadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    public void applyConfig(ServerBootstrap bootstrap) {
        // TODO(NETTY4): actually apply config
    }
}
