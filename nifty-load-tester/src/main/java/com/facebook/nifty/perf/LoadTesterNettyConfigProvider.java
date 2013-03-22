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
package com.facebook.nifty.perf;

import com.facebook.nifty.core.NettyConfigBuilder;
import com.google.inject.Provider;
import org.jboss.netty.channel.socket.ServerSocketChannelConfig;
import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;

public class LoadTesterNettyConfigProvider implements Provider<NettyConfigBuilder> {
    @Override
    public NettyConfigBuilder get() {
        NettyConfigBuilder configBuilder = new NettyConfigBuilder();

        // Some load test client configurations make a *lot* of connections all at once at startup
        configBuilder.getServerSocketChannelConfig().setBacklog(1024);

        // Fastest write throughput is achieved by re-enabling Nagle's algorithm (which is
        // off by default in Netty 4)
        configBuilder.getSocketChannelConfig().setTcpNoDelay(false);

        return configBuilder;
    }
}
