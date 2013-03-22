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

import com.facebook.nifty.client.scribe.LogEntry;
import com.facebook.nifty.client.scribe.ResultCode;
import com.facebook.nifty.client.scribe.scribe;
import com.facebook.nifty.core.NettyConfigBuilder;
import com.facebook.nifty.core.NettyServerTransport;
import com.facebook.nifty.core.ThriftServerDef;
import com.facebook.nifty.core.ThriftServerDefBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;

import java.net.InetSocketAddress;
import java.util.List;

class ScopedNiftyServerRunner<P extends TProcessor> implements AutoCloseable
{
    private final int serverPort;
    private final P processor;
    private NettyServerTransport transport;

    public ScopedNiftyServerRunner(P processor) throws InterruptedException
    {
        this(0, processor);
    }

    public ScopedNiftyServerRunner(int serverPort, P processor) throws InterruptedException
    {
        this.serverPort = serverPort;
        this.processor = processor;
        start();
    }

    public synchronized void start() throws InterruptedException
    {
        ThriftServerDef def = new ThriftServerDefBuilder().listen(serverPort)
                                                          .withProcessor(processor)
                                                          .build();
        ServerBootstrap bootstrap = new ServerBootstrap().group(new NioEventLoopGroup(1), new NioEventLoopGroup(1))
                                                         .channel(NioServerSocketChannel.class);

        transport = new NettyServerTransport(def, new NettyConfigBuilder(), new DefaultChannelGroup());
        transport.start(bootstrap);
    }

    public synchronized InetSocketAddress getLocalAddress()
    {
        NioServerSocketChannel serverSocketChannel = (NioServerSocketChannel) transport.getServerChannel();
        return serverSocketChannel.localAddress();
    }

    public synchronized int getLocalPort()
    {
        return getLocalAddress().getPort();
    }

    public synchronized void close() throws Exception
    {
        transport.stop();
    }
}
