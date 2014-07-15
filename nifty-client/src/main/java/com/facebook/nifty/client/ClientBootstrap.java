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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.SocketAddress;
import java.util.Map;

public interface ClientBootstrap
{
    ClientBootstrap setOption(String option, Object value);

    ClientBootstrap setOptions(Map<String, Object> options);

    ClientBootstrap handler(ChannelHandler handler);

    ClientBootstrap group(NioEventLoopGroup group);

    ChannelFuture connect(SocketAddress address);
}
