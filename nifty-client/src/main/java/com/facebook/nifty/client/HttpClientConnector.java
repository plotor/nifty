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

import com.google.common.net.HostAndPort;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedByteChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecoder;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.util.Timer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HttpClientConnector extends AbstractClientConnector<HttpClientChannel>
{
    private final URI endpointUri;

    public HttpClientConnector(String hostNameAndPort, String servicePath)
            throws URISyntaxException
    {
        super(new InetSocketAddress(HostAndPort.fromString(hostNameAndPort).getHostText(),
                                    HostAndPort.fromString(hostNameAndPort).getPortOrDefault(80)));

        this.endpointUri = new URI("http", hostNameAndPort, servicePath, null, null);
    }

    public HttpClientConnector(URI uri)
    {
        super(HostAndPort.fromParts(checkNotNull(uri).getHost(), getPort(uri)));

        checkArgument(uri.isAbsolute() && !uri.isOpaque(),
                      "HttpClientConnector requires an absolute URI with a path");

        this.endpointUri = uri;
    }

    public static int getPort(URI uri)
    {
        URI uriNN = checkNotNull(uri);
        if (uri.getScheme().toLowerCase().equals("http")) {
            return uriNN.getPort() == -1 ? 80 : uriNN.getPort();
        } else if (uri.getScheme().toLowerCase().equals("https")) {
            return uriNN.getPort() == -1 ? 443 : uriNN.getPort();
        } else {
            throw new IllegalArgumentException("HttpClientConnector only connects to HTTP/HTTPS " +
                                               "URIs");
        }
    }

    @Override
    public HttpClientChannel newThriftClientChannel(Channel nettyChannel, Timer timer)
    {
        HttpClientChannel channel =
                new HttpClientChannel(nettyChannel,
                                      timer,
                                      endpointUri.getHost(),
                                      endpointUri.getPath());
        channel.getNettyChannel().pipeline().addLast("thriftHandler", channel);
        return channel;
    }

    @Override
    public NiftyChannelInitializer<SocketChannel> newChannelInitializer(final int maxFrameSize)
    {
        return new NiftyChannelInitializer<SocketChannel>()
        {
            @Override
            public void initChannel(SocketChannel ch) throws Exception
            {
                ChannelPipeline cp = ch.pipeline();
                cp.addLast("httpClientCodec", new HttpClientCodec());
                cp.addLast("httpContentDecoder", new HttpContentDecompressor());
                cp.addLast("chunkAggregator", new HttpObjectAggregator(maxFrameSize));
            }
        };
    }

    @Override
    public String toString()
    {
        return endpointUri.toString();
    }
}
