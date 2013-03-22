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

import com.facebook.nifty.client.FramedClientChannel;
import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.client.NettyClientConfigBuilder;
import com.facebook.nifty.client.NiftyClient;
import com.facebook.nifty.client.NiftyClientChannel;
import com.facebook.nifty.client.scribe.LogEntry;
import com.facebook.nifty.client.scribe.ResultCode;
import com.facebook.nifty.client.scribe.scribe;
import com.facebook.nifty.core.ThriftMessage;
import com.facebook.nifty.core.ThriftTransportType;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.netty.buffer.Unpooled;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

public class TestSocksClient
{
    // All the thrift messages sent in these tests are pretty small
    private static final int MAX_FRAME_SIZE = 4096;
    private static final Duration DEFAULT_TIMEOUT = Duration.valueOf("2s");
    public static final String LOOPBACK_ADDRESS = "127.0.0.1";

    @Test(timeOut = 5000)
    public void testSocksServerStartupAndShutdown() throws Exception
    {
        try (ScopedSocksServerRunner socksServer = new ScopedSocksServerRunner()) {

            // Verify server started up
            assertTrue(socksServer.getServer().isRunning());
            assertTrue(socksServer.getLocalPort() > 0);

            // Verify we can connect to it
            Socket s = new Socket();
            s.connect(socksServer.getLocalAddress());

        }
    }

    @Test//(timeOut = 5000)
    public void testSyncRequestsThroughSocks() throws Exception
    {
        NettyClientConfigBuilder configBuilder = new NettyClientConfigBuilder();
        configBuilder.setNiftyWorkerThreadCount(1);

        try (ScopedNiftyServerRunner<?> niftyServer = makeScopedNiftyServerRunner();
             ScopedSocksServerRunner socksServer = new ScopedSocksServerRunner();
             NiftyClient niftyClient = new NiftyClient(configBuilder)) {
            InetSocketAddress niftyAddress = new InetSocketAddress(LOOPBACK_ADDRESS, niftyServer.getLocalPort());
            InetSocketAddress socksAddress = new InetSocketAddress(LOOPBACK_ADDRESS, socksServer.getLocalPort());

            TTransport transport = niftyClient.connectSync(niftyAddress,
                                                           DEFAULT_TIMEOUT,
                                                           DEFAULT_TIMEOUT,
                                                           DEFAULT_TIMEOUT,
                                                           MAX_FRAME_SIZE,
                                                           socksAddress);

            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            scribe.Client client = new scribe.Client(protocol);

            client.Log(Lists.<LogEntry>newArrayList());

            transport.close();
        }
    }

    @Test(timeOut = 5000)
    public void testAsyncRequestsThroughSocks() throws Exception
    {
        NettyClientConfigBuilder configBuilder = new NettyClientConfigBuilder();
        configBuilder.setNiftyWorkerThreadCount(1);

        try (ScopedNiftyServerRunner<?> niftyServer = makeScopedNiftyServerRunner();
             ScopedSocksServerRunner socksServer = new ScopedSocksServerRunner();
             NiftyClient niftyClient = new NiftyClient(configBuilder)) {

            InetSocketAddress niftyAddress = new InetSocketAddress(LOOPBACK_ADDRESS, niftyServer.getLocalPort());
            InetSocketAddress socksAddress = new InetSocketAddress(LOOPBACK_ADDRESS, socksServer.getLocalPort());

            ListenableFuture<FramedClientChannel> clientChannelFuture =
                    niftyClient.connectAsync(
                            new FramedClientConnector(niftyAddress),
                            DEFAULT_TIMEOUT,
                            DEFAULT_TIMEOUT,
                            DEFAULT_TIMEOUT,
                            MAX_FRAME_SIZE,
                            socksAddress);

            TMemoryBuffer requestTransport = new TMemoryBuffer(MAX_FRAME_SIZE);
            TBinaryProtocol outProtocol = new TBinaryProtocol(requestTransport);

            byte[] buf = new byte[MAX_FRAME_SIZE];
            final TMemoryInputTransport responseTransport = new TMemoryInputTransport(buf);
            TBinaryProtocol inProtocol = new TBinaryProtocol(responseTransport);

            final AtomicBoolean success = new AtomicBoolean(false);
            final AtomicReference<Exception> exception = new AtomicReference<>(null);
            final CountDownLatch latch = new CountDownLatch(1);
            final scribe.Client client = new scribe.Client(inProtocol, outProtocol);
            client.send_Log(Lists.<LogEntry>newArrayList());

            FramedClientChannel clientChannel = clientChannelFuture.get();
            ThriftMessage request =
                    new ThriftMessage(
                            Unpooled.wrappedBuffer(requestTransport.getArray(), 0, requestTransport.length()),
                            ThriftTransportType.FRAMED);
            clientChannel.sendAsynchronousRequest(
                    request,
                    false,
                    new NiftyClientChannel.Listener()
                    {
                        @Override
                        public void onRequestSent()
                        {
                        }

                        @Override
                        public void onResponseReceived(ThriftMessage message)
                        {
                            try {
                                byte[] buf = responseTransport.getBuffer();
                                message.getBuffer().readBytes(buf, 0, message.getBuffer().readableBytes());
                                client.recv_Log();
                                success.set(true);
                            } catch (TException e) {
                                exception.set(e);
                            } finally {
                                latch.countDown();
                            }
                        }

                        @Override
                        public void onChannelError(TException t)
                        {
                            exception.set(t);
                            latch.countDown();
                        }
                    });

            latch.await();
            clientChannel.close();

            assertNull("The async call hit an error", exception.get());
            assertTrue("Call was not successful", success.get());
        }
    }

    private ScopedNiftyServerRunner<?> makeScopedNiftyServerRunner() throws InterruptedException
    {
        return new ScopedNiftyServerRunner<>(getSimpleScribeProcessor());
    }

    private scribe.Processor<scribe.Iface> getSimpleScribeProcessor()
    {
        scribe.Iface handlerIface = new scribe.Iface()
        {
            @Override
            public ResultCode Log(List<LogEntry> messages)
                    throws TException
            {
                return ResultCode.OK;
            }
        };

        return new scribe.Processor<>(handlerIface);
    }


}
