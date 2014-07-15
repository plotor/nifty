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

import com.facebook.nifty.core.NiftyChannelInitializer;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.google.common.net.HostAndPort;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;

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
        this(hostNameAndPort, servicePath, defaultProtocolFactory());
    }

    public HttpClientConnector(URI uri)
    {
        this(uri, defaultProtocolFactory());
    }

    public HttpClientConnector(String hostNameAndPort, String servicePath, TDuplexProtocolFactory protocolFactory)
            throws URISyntaxException
    {
        super(new InetSocketAddress(HostAndPort.fromString(hostNameAndPort).getHostText(),
                                    HostAndPort.fromString(hostNameAndPort).getPortOrDefault(80)),
              protocolFactory
        );

        this.endpointUri = new URI("http", hostNameAndPort, servicePath, null, null);
    }

    public HttpClientConnector(URI uri, TDuplexProtocolFactory protocolFactory)
    {
        super(toSocketAddress(HostAndPort.fromParts(checkNotNull(uri).getHost(), getPortFromURI(uri))),
              defaultProtocolFactory());

        checkArgument(uri.isAbsolute() && !uri.isOpaque(),
                      "HttpClientConnector requires an absolute URI with a path");

        this.endpointUri = uri;
    }

    @Override
    public HttpClientChannel newThriftClientChannel(Channel nettyChannel, NettyClientConfig clientConfig)
    {
        HttpClientChannel channel =
                new HttpClientChannel(nettyChannel,
                                      clientConfig.getTimer(),
                                      getProtocolFactory(),
                                      endpointUri.getHost(),
                                      endpointUri.getPath());
        channel.getNettyChannel().pipeline().addLast("thriftHandler", channel);
        return channel;
    }

    @Override
    public NiftyChannelInitializer<Channel> newChannelInitializer(int maxFrameSize, NettyClientConfig clientConfig)
    {
        return new NiftyChannelInitializer<Channel>()
        {
            @Override
            public void initChannel(Channel channel) throws Exception
            {
                ChannelPipeline cp = channel.pipeline();
                cp.addLast("httpClientCodec", new HttpClientCodec());
                // TODO(NETTY4): netty4 has no chunk aggregator, but do we need it? try to add a test that sends a chunked http message
                //cp.addLast("chunkAggregator", new HttpChunkAggregator(maxFrameSize));
            }
        };
    }

    @Override
    public String toString()
    {
        return endpointUri.toString();
    }

    private static int getPortFromURI(URI uri)
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
}
