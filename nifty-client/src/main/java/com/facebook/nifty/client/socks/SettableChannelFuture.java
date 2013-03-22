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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;

/**
 * A channel future that allow channel to be set at a later time.
 */
public class SettableChannelFuture extends DefaultChannelPromise
{
    private Channel settableChannel = null;
    private boolean channelIsSet = false;

    public SettableChannelFuture()
    {
        super(null);
    }

    public void setChannel(Channel channel)
    {
        if (!channelIsSet) {
            this.settableChannel = channel;
            this.channelIsSet = true;
        }
    }

    @Override
    public Channel channel()
    {
        return settableChannel;
    }

    @Override
    public ChannelPromise setFailure(Throwable cause)
    {
        if (!this.channelIsSet) {
            throw new IllegalStateException("channel not set yet !");
        }
        return super.setFailure(cause);
    }

    @Override
    public ChannelPromise setSuccess(Void result)
    {
        if (!this.channelIsSet) {
            throw new IllegalStateException("channel not set yet !");
        }
        return super.setSuccess(result);
    }

    @Override
    protected void checkDeadLock()
    {
        if (this.channelIsSet) {
            super.checkDeadLock();
        }
    }
}
