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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NiftyNoOpSecurityFactory implements NiftySecurityFactory {

    static final ChannelHandler noOpHandler = new ChannelInboundHandlerAdapter() {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            super.channelRegistered(ctx);
            ctx.pipeline().remove(this);
        }
    };

    @Override
    public NiftySecurityHandlers getSecurityHandlers(ThriftServerDef def, NettyServerConfig serverConfig) {
        return new NiftySecurityHandlers() {
            @Override
            public ChannelHandler getAuthenticationHandler() {
                return noOpHandler;
            }

            @Override
            public ChannelHandler getEncryptionHandler() {
                return noOpHandler;
            }
        };
    }
}
