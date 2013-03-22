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

import com.google.common.base.Throwables;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.example.socksproxy.SocksServerInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An example SOCKS server for testing the nifty SOCKS client, adapted from the netty SOCKS server
 * example code.
 */
public final class ExampleSocksServer extends Thread {
    private static Logger log = LoggerFactory.getLogger(ExampleSocksServer.class);

    private final int serverPort;
    private NioServerSocketChannel channel;
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    private AtomicBoolean isStarted = new AtomicBoolean(false);

    public ExampleSocksServer()
    {
        this(0);
    }

    public ExampleSocksServer(int serverPort)
    {
        this.serverPort = serverPort;
    }

    public void run() {
        ServerBootstrap b = new ServerBootstrap();
        final NioEventLoopGroup parentGroup = new NioEventLoopGroup();
        final NioEventLoopGroup childGroup = new NioEventLoopGroup();
        try {
            synchronized (this) {
                b.group(parentGroup, childGroup)
                 .channel(NioServerSocketChannel.class)
                 .childHandler(new SocksServerInitializer());
                channel = (NioServerSocketChannel) b.bind(serverPort).sync().channel();
                notifyStarted();
            }
            channel.closeFuture().sync();
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            b.shutdown();
            try {
                Thread parentWaiter = new Thread() {
                    @Override
                    public void run()
                    {
                        try {
                            parentGroup.awaitTermination(1, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            log.warn("Interrupted while shutting down parent NioEventLoopGroup");
                        }
                    }
                };
                Thread childWaiter = new Thread() {
                    @Override
                    public void run()
                    {
                        try {
                            childGroup.awaitTermination(1, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            log.warn("Interrupted while shutting down child NioEventLoopGroup");
                        }
                    }
                };
                parentWaiter.start();
                childWaiter.start();
                parentWaiter.join();
                childWaiter.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                notifyShutdown();
            }
        }
    }

    private void notifyShutdown()
    {
        synchronized (isShutdown) {
            isShutdown.set(true);
            isShutdown.notifyAll();
        }
    }

    private void notifyStarted()
    {
        synchronized (isStarted) {
            isStarted.set(true);
            isStarted.notifyAll();
        }
    }

    public void waitForStartup() throws InterruptedException {
        synchronized (isStarted) {
            while (!isStarted.get()) {
                isStarted.wait();
            }
        }
    }

    public void waitForShutdown() throws InterruptedException {
        synchronized (isShutdown) {
            while (!isShutdown.get()) {
                isShutdown.wait();
            }
        }
    }

    public synchronized void shutdown() {
        channel.close();
    }

    public synchronized boolean isRunning() {
        return isStarted.get() && channel.isActive();
    }

    public synchronized InetSocketAddress getLocalAddress() {
        return channel.localAddress();
    }

    public synchronized int getLocalPort() {
        return getLocalAddress().getPort();
    }
}
