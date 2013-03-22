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
package com.facebook.nifty.server;

import com.facebook.nifty.client.FramedClientChannel;
import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.client.NettyClientConfigBuilder;
import com.facebook.nifty.client.NiftyClient;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.netty.channel.ConnectTimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.fail;

public class TestNiftyClientTimeout
{
    private static final Duration TEST_CONNECT_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final Duration TEST_READ_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final Duration TEST_WRITE_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final int TEST_MAX_FRAME_SIZE = 16777216;

    @BeforeTest(alwaysRun = true)
    public void init() {
      // must load DelegateSelectorProvider before entire test suite to
      // properly wire up selector provider.
      DelegateSelectorProvider.init();
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() {
      DelegateSelectorProvider.makeDeaf();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
      DelegateSelectorProvider.makeUndeaf();
    }

    @Test(timeOut = 5000)
    public void testSyncConnectTimeout() throws ConnectException, IOException
    {
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();
        final NettyClientConfigBuilder configBuilder = new NettyClientConfigBuilder();
        configBuilder.setNiftyWorkerThreadCount(1);
        final NiftyClient client = new NiftyClient(configBuilder);
        try {
                client.connectSync(new InetSocketAddress(port),
                                   TEST_CONNECT_TIMEOUT,
                                   TEST_READ_TIMEOUT,
                                   TEST_WRITE_TIMEOUT,
                                   TEST_MAX_FRAME_SIZE);
        }
        catch (Throwable throwable) {
            if (isTimeoutException(throwable)) {
                return;
            }
            Throwables.propagate(throwable);
        }
        finally {
            // Shutting down the netty NioEventLoopGroup will wait up to 2 seconds after the last
            // event sent to the channel (in this case, the connect attempt). So the timeout for the
            // test must take this into account and be *at least* TEST_CONNECT_TIMEOUT + 2 seconds
            client.close();
            serverSocket.close();
        }

        // Should never get here
        fail("Connection succeeded but failure was expected");
    }

    @Test(timeOut = 5000)
    public void testAsyncConnectTimeout() throws IOException
    {
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();

        final NiftyClient client = new NiftyClient();
        try {
            ListenableFuture<FramedClientChannel> future =
                            client.connectAsync(new FramedClientConnector(new InetSocketAddress(port)),
                                                TEST_CONNECT_TIMEOUT,
                                                TEST_READ_TIMEOUT,
                                                TEST_WRITE_TIMEOUT,
                                                TEST_MAX_FRAME_SIZE);
            // Wait while NiftyClient attempts to connect the channel
            future.get();
        }
        catch (Throwable throwable) {
            if (isTimeoutException(throwable)) {
                return;
            }
            Throwables.propagate(throwable);
        }
        finally {
            // Shutting down the netty NioEventLoopGroup will wait up to 2 seconds after the last
            // event sent to the channel (in this case, the connect attempt). So the timeout for the
            // test must take this into account and be *at least* TEST_CONNECT_TIMEOUT + 2 seconds
            client.close();
            serverSocket.close();
        }

        // Should never get here
        fail("Connection succeeded but failure was expected");
    }

    private boolean isTimeoutException(Throwable throwable) {
        Throwable rootCause = Throwables.getRootCause(throwable);
        return (rootCause instanceof ConnectTimeoutException);
    }
}
