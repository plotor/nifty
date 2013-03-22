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

import com.facebook.nifty.core.ThriftUnframedDecoder;
import com.google.common.net.HostAndPort;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.Timer;

import java.net.InetSocketAddress;

public class UnframedClientConnector extends AbstractClientConnector<UnframedClientChannel> {
    public UnframedClientConnector(InetSocketAddress address) {
        super(address);
    }

    public UnframedClientConnector(HostAndPort address) {
        super(address);
    }

    @Override
    public UnframedClientChannel newThriftClientChannel(Channel nettyChannel, Timer timer) {
        UnframedClientChannel channel = new UnframedClientChannel(nettyChannel, timer);
        channel.getNettyChannel().pipeline().addLast("thriftHandler", channel);
        return channel;
    }

    @Override
    public NiftyChannelInitializer<SocketChannel> newChannelInitializer(final int maxFrameSize) {
        return new NiftyChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception
            {
                ChannelPipeline cp = ch.pipeline();
                cp.addLast("thriftUnframedDecoder", new ThriftUnframedDecoder());
            }
        };
    }
}
