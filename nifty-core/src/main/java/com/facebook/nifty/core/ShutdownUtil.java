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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ChannelFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.group.ChannelGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ShutdownUtil
{
    private static final Logger log = LoggerFactory.getLogger(ShutdownUtil.class);

    public static void shutdownChannelFactory(ChannelFactory channelFactory,
                                              EventExecutorGroup bossExecutor,
                                              EventExecutorGroup workerExecutor,
                                              ChannelGroup allChannels)
    {
        // Close all channels
        if (allChannels != null) {
            closeChannels(allChannels);
        }

        //// Shutdown the channel factory
        //if (channelFactory != null) {
        //    channelFactory.shutdown();
        //}

        // Stop boss threads
        if (bossExecutor != null) {
            shutdownExecutor(bossExecutor, "bossExecutor");
        }

        // Finally stop I/O workers
        if (workerExecutor != null) {
            shutdownExecutor(workerExecutor, "workerExecutor");
        }

        //// Release any other resources netty might be holding onto via this channelFactory
        //if (channelFactory != null) {
        //    channelFactory.releaseExternalResources();
        //}
    }

    public static void shutdownChannelFactory(Bootstrap bootstrap, ChannelGroup allChannels)
    {
        // TODO(NETTY4): implement this
        try {
            bootstrap.group().shutdownGracefully(100, 200, TimeUnit.MILLISECONDS).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void shutdownChannelFactory(ServerBootstrap bootstrap, ChannelGroup allChannels)
    {
        // TODO(NETTY4): fix this
        try {
            bootstrap.group().shutdownGracefully(100, 200, TimeUnit.MILLISECONDS).get();
            bootstrap.childGroup().shutdownGracefully(100, 200, TimeUnit.MILLISECONDS).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void closeChannels(ChannelGroup allChannels)
    {
        if (allChannels.size() > 0)
        {
            // TODO : allow an option here to control if we need to drain connections and wait instead of killing them all
            try {
                log.info("Closing " + allChannels.size() + " open client connections");
                if (!allChannels.close().await(5, TimeUnit.SECONDS)) {
                    log.warn("Failed to close all open client connections");
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while closing client connections");
                Thread.currentThread().interrupt();
            }
        }
    }

    // TODO : make wait time configurable ?
    public static void shutdownExecutor(EventExecutorGroup executor, final String name)
    {
        Future<?> terminationFuture = executor.shutdownGracefully();
        try {
            log.info("Waiting for {} to shutdown", name);
            //if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            try {
                terminationFuture.get(5, TimeUnit.SECONDS);
            }
            catch (TimeoutException | ExecutionException e) {
                log.warn("{} did not shutdown properly", name);
            }
        }
        catch (InterruptedException e) {
            log.warn("Interrupted while waiting for {} to shutdown", name);
            Thread.currentThread().interrupt();
        }
    }

    // TODO : make wait time configurable ?
    public static void shutdownExecutor(ExecutorService executor, final String name)
    {
        executor.shutdown();
        try {
            log.info("Waiting for {} to shutdown", name);
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("{} did not shutdown properly", name);
            }
        }
        catch (InterruptedException e) {
            log.warn("Interrupted while waiting for {} to shutdown", name);
            Thread.currentThread().interrupt();
        }
    }
}
