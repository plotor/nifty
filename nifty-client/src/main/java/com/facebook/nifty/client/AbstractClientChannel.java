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
import io.airlift.units.Duration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.WriteTimeoutException;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public abstract class AbstractClientChannel<ResponseType>
        extends ChannelInboundMessageHandlerAdapter<ResponseType>
        implements NiftyClientChannel {
    private final Channel nettyChannel;
    private Duration sendTimeout = null;
    private Duration requestTimeout = null;
    private final Map<Integer, Request> requestMap = new HashMap<Integer, Request>();
    private volatile TException channelError;
    private final Timer timer;

    public AbstractClientChannel(Channel nettyChannel, Timer timer) {
        this.nettyChannel = nettyChannel;
        this.timer = timer;
    }

    @Override
    public Channel getNettyChannel() {
        return nettyChannel;
    }

    @Override
    public SocketAddress getRemoteAddress()
    {
        return nettyChannel.remoteAddress();
    }

    protected abstract ThriftMessage extractResponse(ResponseType message) throws TTransportException;

    protected abstract int extractSequenceId(ThriftMessage message)
            throws TTransportException;

    protected abstract ChannelFuture writeRequest(ThriftMessage request);

    public void close()
    {
        getNettyChannel().close();
    }

    @Override
    public void setSendTimeout(Duration sendTimeout)
    {
        this.sendTimeout = sendTimeout;
    }

    @Override
    public Duration getSendTimeout()
    {
        return sendTimeout;
    }

    @Override
    public void setReceiveTimeout(Duration receiveTimeout)
    {
        this.requestTimeout = receiveTimeout;
    }

    @Override
    public Duration getReceiveTimeout()
    {
        return this.requestTimeout;
    }

    @Override
    public boolean hasError()
    {
        return channelError != null;
    }

    @Override
    public TException getError()
    {
        return channelError;
    }

    @Override
    public void executeInIoThread(Runnable runnable)
    {
        NioSocketChannel nioSocketChannel = (NioSocketChannel) getNettyChannel();
        nioSocketChannel.eventLoop().execute(runnable);
    }

    private boolean hasRequestTimeout()
    {
        return requestTimeout != null;
    }

    @Override
    public void sendAsynchronousRequest(final ThriftMessage message,
                                        final boolean oneway,
                                        final Listener listener)
            throws TException
    {
        final int sequenceId = extractSequenceId(message);

        // Ensure channel listeners are always called on the channel's I/O thread
        executeInIoThread(new Runnable()
        {
            @Override
            public void run()
            {
                ChannelFuture sendFuture = writeRequest(message);
                final Request request = makeRequest(sequenceId, listener);
                queueSendTimeout(request);

                sendFuture.addListener(new ChannelFutureListener()
                {
                    @Override
                    public void operationComplete(ChannelFuture future)
                            throws Exception
                    {
                        if (future.isSuccess()) {
                            cancelRequestTimeouts(request);
                            listener.onRequestSent();
                            if (oneway) {
                                retireRequest(sequenceId);
                            } else {
                                queueReceiveTimeout(request);
                            }
                        } else {
                            TTransportException transportException =
                                    new TTransportException("Sending request failed",
                                                            future.cause());
                            listener.onChannelError(transportException);
                            onError(transportException);
                        }
                    }
                });
            }
        });
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, ResponseType response)
    {
        try {
            int sequenceId = extractSequenceId(extractResponse(response));
            onResponseReceived(sequenceId, extractResponse(response));
        }
        catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t)
            throws Exception
    {
        ctx.channel().close();
        onError(t);
    }

    private Request makeRequest(int sequenceId, Listener listener)
    {
        Request request = new Request(sequenceId, listener);
        requestMap.put(sequenceId, request);
        return request;
    }

    private Request retireRequest(int sequenceId)
    {
        Request request = requestMap.remove(sequenceId);
        cancelRequestTimeouts(request);
        return request;
    }

    private void cancelRequestTimeouts(Request request)
    {
        Timeout sendTimeout = request.getSendTimeout();
        if (sendTimeout != null) {
            sendTimeout.cancel();
        }

        Timeout responseTimeout = request.getReceiveTimeout();
        if (responseTimeout != null) {
            responseTimeout.cancel();
        }
    }

    private void cancelAllTimeouts()
    {
        for (Request request : requestMap.values()) {
            cancelRequestTimeouts(request);
        }
    }

    private void onResponseReceived(int sequenceId, ThriftMessage response)
    {
        Request request = retireRequest(sequenceId);
        if (request == null) {
            onError(new TTransportException("Bad sequence id in response: " + sequenceId));
        } else {
            request.getListener().onResponseReceived(response);
        }
    }

    protected void onError(Throwable t)
    {
        TException wrappedException = wrapException(t);

        if (channelError == null) {
            channelError = wrappedException;
        }

        cancelAllTimeouts();

        Collection<Request> requests = new ArrayList<Request>();
        requests.addAll(requestMap.values());
        requestMap.clear();
        for (Request request : requests) {
            request.getListener().onChannelError(wrappedException);
        }
    }

    protected TException wrapException(Throwable t)
    {
        if (t instanceof TException) {
            return (TException) t;
        } else {
            return new TException(t);
        }
    }

    private void onSendTimeoutExpired(Request request)
    {
        Timeout expiredTimeout = request.getSendTimeout();

        if (!expiredTimeout.isCancelled()) {
            cancelAllTimeouts();
            String message = "Timed out waiting " + getSendTimeout() + " to send request";
            request.getListener().onChannelError(new TTransportException(message, WriteTimeoutException.INSTANCE));
        }
    }

    private void onReceiveTimeoutExpired(Request request)
    {
        Timeout expiredTimeout = request.getReceiveTimeout();

        if (!expiredTimeout.isCancelled()) {
            cancelAllTimeouts();

            String message = "Timed out waiting " + getReceiveTimeout() + " to receive response";
            request.getListener().onChannelError(new TTransportException(message, ReadTimeoutException.INSTANCE));
        }
    }

    private void queueSendTimeout(final Request request)
    {
        if (this.sendTimeout != null) {
            double sendTimeoutMs = this.sendTimeout.toMillis();
            if (sendTimeoutMs > 0) {
                TimerTask sendTimeoutTask = new IoThreadBoundTimerTask(this, new TimerTask() {
                    @Override
                    public void run(Timeout timeout) {
                        onSendTimeoutExpired(request);
                    }
                });

                Timeout sendTimeout = timer.newTimeout(sendTimeoutTask,
                                                       (long) sendTimeoutMs,
                                                       TimeUnit.MILLISECONDS);
                request.setSendTimeout(sendTimeout);
            }
        }
    }

    private void queueReceiveTimeout(final Request request)
    {
        if (this.requestTimeout != null) {
            double requestTimeoutMs = this.requestTimeout.toMillis();
            if (requestTimeoutMs > 0) {
                TimerTask receiveTimeoutTask = new IoThreadBoundTimerTask(this, new TimerTask() {
                    @Override
                    public void run(Timeout timeout) {
                        onReceiveTimeoutExpired(request);
                    }
                });

                Timeout timeout = timer.newTimeout(receiveTimeoutTask,
                                                   (long) requestTimeoutMs,
                                                   TimeUnit.MILLISECONDS);
                request.setReceiveTimeout(timeout);
            }
        }
    }

    /**
     * Used to create TimerTasks that will fire
     */
    private static class IoThreadBoundTimerTask implements TimerTask {
        private final NiftyClientChannel channel;
        private final TimerTask timerTask;

        public IoThreadBoundTimerTask(NiftyClientChannel channel, TimerTask timerTask)
        {
            this.channel = channel;
            this.timerTask = timerTask;
        }

        @Override
        public void run(final Timeout timeout)
                throws Exception
        {
            channel.executeInIoThread(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        timerTask.run(timeout);
                    } catch (Exception e) {
                        channel.getNettyChannel().pipeline().fireExceptionCaught(e);
                    }
                }
            });
        }
    }

    /**
     * Bundles the details of a client request that has started, but for which a response hasn't
     * yet been received (or in the one-way case, the send operation hasn't completed yet).
     */
    private static class Request
    {
        private final int sequenceId;
        private final Listener listener;
        private Timeout sendTimeout;
        private Timeout receiveTimeout;

        public Request(int sequenceId, Listener listener)
        {
            this.sequenceId = sequenceId;
            this.listener = listener;
        }

        public int getSequenceId()
        {
            return sequenceId;
        }

        public Listener getListener()
        {
            return listener;
        }

        public Timeout getReceiveTimeout()
        {
            return receiveTimeout;
        }

        public void setReceiveTimeout(Timeout receiveTimeout)
        {
            this.receiveTimeout = receiveTimeout;
        }

        public Timeout getSendTimeout()
        {
            return sendTimeout;
        }

        public void setSendTimeout(Timeout sendTimeout)
        {
            this.sendTimeout = sendTimeout;
        }
    }
}
