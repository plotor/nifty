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

import com.facebook.nifty.core.ThriftMessage;
import com.facebook.nifty.core.ThriftTransportType;
import org.apache.thrift.transport.TTransportException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.Timer;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class UnframedClientChannel extends AbstractClientChannel<ByteBuf> {
    public UnframedClientChannel(Channel channel, Timer timer) {
        super(channel, timer);
    }

    @Override
    protected ThriftMessage extractResponse(ByteBuf buffer) {
        if (!buffer.isReadable()) {
            return null;
        }

        return new ThriftMessage(buffer, getTransportType());
    }

    @Override
    protected int extractSequenceId(ThriftMessage message) throws TTransportException {
        try {
            int sequenceId;
            int stringLength;
            stringLength = message.getBuffer().getInt(4);
            sequenceId = message.getBuffer().getInt(8 + stringLength);
            return sequenceId;
        } catch (Throwable t) {
            throw new TTransportException("Could not find sequenceId in Thrift message");
        }
    }

    @Override
    protected ChannelFuture writeRequest(ThriftMessage request) {
        return getNettyChannel().write(request.getBuffer());
    }

    @Override
    public ThriftTransportType getTransportType()
    {
        return ThriftTransportType.UNFRAMED;
    }
}
