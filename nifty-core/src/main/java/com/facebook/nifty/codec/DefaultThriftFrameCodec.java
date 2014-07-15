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
package com.facebook.nifty.codec;

import com.facebook.nifty.core.ThriftMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.List;

public class DefaultThriftFrameCodec extends ThriftFrameCodec
{
    private final ThriftFrameDecoder decoder;
    private final ThriftFrameEncoder encoder;

    public DefaultThriftFrameCodec(int maxFrameSize, TProtocolFactory inputProtocolFactory)
    {
        this.decoder = new DefaultThriftFrameDecoder(maxFrameSize, inputProtocolFactory);
        this.encoder = new DefaultThriftFrameEncoder(maxFrameSize);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        decoder.decode(ctx, in, out);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ThriftMessage message, ByteBuf out) throws Exception
    {
        encoder.encode(ctx, message, out);
    }
}
