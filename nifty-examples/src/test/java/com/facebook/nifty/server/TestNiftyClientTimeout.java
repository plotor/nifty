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
import com.facebook.nifty.client.NiftyClient;
import com.facebook.nifty.test.scribe;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.netty.channel.ConnectTimeoutException;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import static org.testng.Assert.fail;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class TestNiftyClientTimeout {
    private static final Duration TEST_CONNECT_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final Duration TEST_RECEIVE_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final Duration TEST_READ_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final Duration TEST_SEND_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final int TEST_MAX_FRAME_SIZE = 16777216;

    @BeforeTest(alwaysRun = true)
    public void init() {
        // must load DelegateSelectorProvider before entire test suite to
        // properly wire up selector provider.
        DelegateSelectorProvider.init();
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        DelegateSelectorProvider.makeDeaf();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        DelegateSelectorProvider.makeUndeaf();
    }

    @Test(timeOut = 10000)
    public void testSyncConnectTimeout() throws ConnectException, IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();
        final NiftyClient client = new NiftyClient();
        try {
            client.connectSync(scribe.Client.class,
                    new FramedClientConnector(new InetSocketAddress(port)),
                    TEST_CONNECT_TIMEOUT,
                    TEST_RECEIVE_TIMEOUT,
                    TEST_SEND_TIMEOUT,
                    TEST_MAX_FRAME_SIZE);//.setReadTimeout(TEST_READ_TIMEOUT);
        } catch (Throwable throwable) {
            if (this.isTimeoutException(throwable)) {
                return;
            }
            Throwables.propagate(throwable);
        } finally {
            client.close();
            serverSocket.close();
        }

        // Should never get here
        fail("Connection succeeded but failure was expected");
    }

    @Test(timeOut = 5000)
    public void testAsyncConnectTimeout() throws IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();

        final NiftyClient client = new NiftyClient();
        try {
            ListenableFuture<FramedClientChannel> future =
                    client.connectAsync(new FramedClientConnector(new InetSocketAddress(port)),
                            TEST_CONNECT_TIMEOUT,
                            TEST_RECEIVE_TIMEOUT,
                            TEST_SEND_TIMEOUT,
                            TEST_MAX_FRAME_SIZE);
            future = Futures.transform(future,
                    new Function<FramedClientChannel, FramedClientChannel>() {
                        @Nullable
                        @Override
                        public FramedClientChannel apply(@Nullable FramedClientChannel input) {
                            input.setReadTimeout(TEST_READ_TIMEOUT);
                            return input;
                        }
                    });

            // Wait while NiftyClient attempts to connect the channel
            future.get();
        } catch (Throwable throwable) {
            if (this.isTimeoutException(throwable)) {
                return;
            }
            Throwables.propagate(throwable);
        } finally {
            client.close();
            serverSocket.close();
        }

        // Should never get here
        fail("Connection succeeded but failure was expected");
    }

    private boolean isTimeoutException(Throwable throwable) {
        Throwable rootCause = Throwables.getRootCause(throwable);
        // Look for a java.net.ConnectException, with the message "connection timed out"
        return rootCause instanceof ConnectTimeoutException &&
                rootCause.getMessage().startsWith("connection timed out");
    }
}
