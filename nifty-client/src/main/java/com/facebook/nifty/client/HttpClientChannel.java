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
import com.google.common.net.HttpHeaders;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import org.apache.thrift.transport.TTransportException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.Timer;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;

@NotThreadSafe
public class HttpClientChannel extends AbstractClientChannel<FullHttpResponse> {
    private final Channel underlyingNettyChannel;
    private final String hostName;
    private Map<String, String> headerDictionary;
    private final String endpointUri;

    public HttpClientChannel(Channel channel,
                             Timer timer,
                             String hostName,
                             String endpointUri) {
        super(channel, timer);

        this.underlyingNettyChannel = channel;
        this.hostName = hostName;
        this.endpointUri = endpointUri;
    }

    @Override
    protected int extractSequenceId(ThriftMessage message)
            throws TTransportException
    {
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
    public Channel getNettyChannel() {
        return underlyingNettyChannel;
    }

    @Override
    public ThriftTransportType getTransportType()
    {
        return ThriftTransportType.HTTP;
    }


    @Override
    protected ThriftMessage extractResponse(FullHttpResponse response) throws TTransportException
    {
        if (!response.getStatus().equals(HttpResponseStatus.OK)) {
            throw new TTransportException("HTTP response had non-OK status: " + response.getStatus().toString());
        }

        ByteBuf content = response.data().copy();

        if (!content.isReadable()) {
            return null;
        }

        return new ThriftMessage(content, getTransportType());
    }

    @Override
    protected ChannelFuture writeRequest(ThriftMessage request)
    {
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                                                 HttpMethod.POST,
                                                                 endpointUri,
                                                                 request.getBuffer());

        httpRequest.headers().set(HttpHeaders.HOST, hostName);
        httpRequest.headers().set(HttpHeaders.CONTENT_LENGTH, request.getBuffer().readableBytes());
        httpRequest.headers().set(HttpHeaders.CONTENT_TYPE, "application/x-thrift");
        httpRequest.headers().set(HttpHeaders.ACCEPT, "application/x-thrift");
            httpRequest.headers().set(HttpHeaders.USER_AGENT, "Java/Swift-HttpThriftClientChannel");

        if (headerDictionary != null) {
            for (Map.Entry<String, String> entry : headerDictionary.entrySet()) {
                httpRequest.headers().set(entry.getKey(), entry.getValue());
            }
        }

        return underlyingNettyChannel.write(httpRequest);
    }

    public void setHeaders(Map<String, String> headers)
    {
        this.headerDictionary = headers;
    }

}
