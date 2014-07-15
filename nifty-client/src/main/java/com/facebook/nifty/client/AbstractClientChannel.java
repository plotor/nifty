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

import com.facebook.nifty.core.ByteBufInputTransport;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.WriteTimeoutException;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public abstract class AbstractClientChannel extends ChannelDuplexHandler implements
        NiftyClientChannel {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClientChannel.class);

    private final Channel nettyChannel;
    private Duration sendTimeout = null;

    // Timeout until the whole request must be received.
    private Duration receiveTimeout = null;

    // Timeout for not receiving any data from the server
    private Duration readTimeout = null;

    private final Map<Integer, Request> requestMap = new HashMap<>();
    private volatile TException channelError;
    private final Timer timer;
    private final TDuplexProtocolFactory protocolFactory;

    protected AbstractClientChannel(Channel nettyChannel, Timer timer, TDuplexProtocolFactory protocolFactory) {
        this.nettyChannel = nettyChannel;
        this.timer = timer;
        this.protocolFactory = protocolFactory;
    }

    @Override
    public Channel getNettyChannel() {
        return nettyChannel;
    }

    @Override
    public TDuplexProtocolFactory getProtocolFactory()
    {
        return protocolFactory;
    }

    protected abstract ByteBuf extractResponse(Object message) throws TTransportException;

    protected int extractSequenceId(ByteBuf messageBuffer)
            throws TTransportException
    {
        try {
            messageBuffer.markReaderIndex();
            TTransport inputTransport = new ByteBufInputTransport(messageBuffer);
            TProtocol inputProtocol = getProtocolFactory().getInputProtocolFactory().getProtocol(inputTransport);
            TMessage message = inputProtocol.readMessageBegin();
            messageBuffer.resetReaderIndex();
            return message.seqid;
        } catch (Throwable t) {
            throw new TTransportException("Could not find sequenceId in Thrift message");
        }
    }

    protected abstract ChannelFuture writeRequest(ByteBuf request);

    public void close()
    {
        getNettyChannel().close();
    }

    @Override
    public void setSendTimeout(@Nullable Duration sendTimeout)
    {
        this.sendTimeout = sendTimeout;
    }

    @Override
    public Duration getSendTimeout()
    {
        return sendTimeout;
    }

    @Override
    public void setReceiveTimeout(@Nullable Duration receiveTimeout)
    {
        this.receiveTimeout = receiveTimeout;
    }

    @Override
    public Duration getReceiveTimeout()
    {
        return receiveTimeout;
    }

    @Override
    public void setReadTimeout(@Nullable Duration readTimeout)
    {
        this.readTimeout = readTimeout;
    }

    @Override
    public Duration getReadTimeout()
    {
        return this.readTimeout;
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

    @Override
    public void sendAsynchronousRequest(final ByteBuf message,
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
                try {
                    final Request request = makeRequest(sequenceId, listener);

                    if (!nettyChannel.isActive()) {
                        fireChannelErrorCallback(listener, new TTransportException(TTransportException.NOT_OPEN, "Channel closed"));
                        return;
                    }

                    if (hasError()) {
                        fireChannelErrorCallback(
                                listener,
                                new TTransportException(TTransportException.UNKNOWN, "Channel is in a bad state due to failing a previous request"));
                        return;
                    }

                    ChannelFuture sendFuture = writeRequest(message);
                    queueSendTimeout(request);

                    getNettyChannel().flush();

                    sendFuture.addListener(new ChannelFutureListener()
                    {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception
                        {
                            messageSent(future, request, oneway);
                        }
                    });
                }
                catch (Throwable t) {
                    // onError calls all registered listeners in the requestMap, but this request
                    // may not be registered yet. So we try to remove it (to make sure we don't call
                    // the callback twice) and then manually make the callback for this request
                    // listener.
                    requestMap.remove(sequenceId);
                    fireChannelErrorCallback(listener, t);

                    onError(t);
                }
            }
        });
    }

    private void messageSent(ChannelFuture future, Request request, boolean oneway)
    {
        try {
            if (future.isSuccess()) {
                cancelRequestTimeouts(request);
                fireRequestSentCallback(request.getListener());
                if (oneway) {
                    retireRequest(request);
                } else {
                    queueReceiveAndReadTimeout(request);
                }
            } else {
                TTransportException transportException =
                        new TTransportException("Sending request failed",
                                                future.cause());
                onError(transportException);
            }
        }
        catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        try {
            ByteBuf response = extractResponse(msg);

            if (response != null) {
                int sequenceId = extractSequenceId(response);
                onResponseReceived(sequenceId, response);
            }
            else {
                super.channelRead(ctx, msg);
            }
        }
        catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception
    {
        onError(cause);
        ctx.channel().close();
    }

    private Request makeRequest(int sequenceId, Listener listener)
    {
        Request request = new Request(listener);
        requestMap.put(sequenceId, request);
        return request;
    }

    private void retireRequest(Request request)
    {
        cancelRequestTimeouts(request);
    }

    private void cancelRequestTimeouts(Request request)
    {
        Timeout sendTimeout = request.getSendTimeout();
        if (sendTimeout != null && !sendTimeout.isCancelled()) {
            sendTimeout.cancel();
        }

        Timeout receiveTimeout = request.getReceiveTimeout();
        if (receiveTimeout != null && !receiveTimeout.isCancelled()) {
            receiveTimeout.cancel();
        }

        Timeout readTimeout = request.getReadTimeout();
        if (readTimeout != null && !readTimeout.isCancelled()) {
            readTimeout.cancel();
        }
    }

    private void cancelAllTimeouts()
    {
        for (Request request : requestMap.values()) {
            cancelRequestTimeouts(request);
        }
    }

    private void onResponseReceived(int sequenceId, ByteBuf response)
    {
        Request request = requestMap.remove(sequenceId);
        if (request == null) {
            onError(new TTransportException("Bad sequence id in response: " + sequenceId));
        } else {
            retireRequest(request);
            fireResponseReceivedCallback(request.getListener(), response);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        if (!requestMap.isEmpty()) {
            onError(new TTransportException("Client was disconnected by server"));
        }
    }

    protected void onError(Throwable t)
    {
        TException wrappedException = wrapException(t);

        if (channelError == null) {
            channelError = wrappedException;
        }

        cancelAllTimeouts();

        Collection<Request> requests = new ArrayList<>();
        requests.addAll(requestMap.values());
        requestMap.clear();
        for (Request request : requests) {
            fireChannelErrorCallback(request.getListener(), wrappedException);
        }
    }

    protected TException wrapException(Throwable t)
    {
        if (t instanceof TException) {
            return (TException) t;
        } else {
            return new TTransportException(t);
        }
    }

    private void fireRequestSentCallback(Listener listener)
    {
        try {
            listener.onRequestSent();
        }
        catch (Throwable t) {
            LOGGER.warn("Request sent listener callback triggered an exception: {}", t);
        }
    }

    private void fireResponseReceivedCallback(Listener listener, ByteBuf response)
    {
        try {
            listener.onResponseReceived(response);
        }
        catch (Throwable t) {
            LOGGER.warn("Response received listener callback triggered an exception: {}", t);
        }
    }

    private void fireChannelErrorCallback(Listener listener, TException exception)
    {
        try {
            listener.onChannelError(exception);
        }
        catch (Throwable t) {
            LOGGER.warn("Channel error listener callback triggered an exception: {}", t);
        }
    }

    private void fireChannelErrorCallback(Listener listener, Throwable throwable)
    {
        fireChannelErrorCallback(listener, wrapException(throwable));
    }

    private void onSendTimeoutFired(Request request)
    {
        cancelAllTimeouts();
        fireChannelErrorCallback(request.getListener(), new TTransportException(TTransportException.TIMED_OUT,
                                                                                "Timed out waiting " + getSendTimeout() + " to send data to server",
                                                                                WriteTimeoutException.INSTANCE));
    }

    private void onReceiveTimeoutFired(Request request)
    {
        cancelAllTimeouts();
        fireChannelErrorCallback(request.getListener(), new TTransportException(TTransportException.TIMED_OUT,
                                                                                "Timed out waiting " + getReceiveTimeout() + " to receive response",
                                                                                ReadTimeoutException.INSTANCE));
    }

    private void onReadTimeoutFired(Request request)
    {
        cancelAllTimeouts();
        fireChannelErrorCallback(request.getListener(), new TTransportException(TTransportException.TIMED_OUT,
                                                                                "Timed out waiting " + getReadTimeout() + " to read data from server",
                                                                                ReadTimeoutException.INSTANCE));
    }


    private void queueSendTimeout(final Request request) throws TTransportException
    {
        if (this.sendTimeout != null) {
            long sendTimeoutMs = this.sendTimeout.toMillis();
            if (sendTimeoutMs > 0) {
                TimerTask sendTimeoutTask = new IoThreadBoundTimerTask(this, new TimerTask() {
                    @Override
                    public void run(Timeout timeout) {
                        onSendTimeoutFired(request);
                    }
                });

                Timeout sendTimeout;
                try {
                    sendTimeout = timer.newTimeout(sendTimeoutTask, sendTimeoutMs, TimeUnit.MILLISECONDS);
                }
                catch (IllegalStateException e) {
                    throw new TTransportException("Unable to schedule send timeout");
                }
                request.setSendTimeout(sendTimeout);
            }
        }
    }

    private void queueReceiveAndReadTimeout(final Request request) throws TTransportException
    {
        if (this.receiveTimeout != null) {
            long receiveTimeoutMs = this.receiveTimeout.toMillis();
            if (receiveTimeoutMs > 0) {
                TimerTask receiveTimeoutTask = new IoThreadBoundTimerTask(this, new TimerTask() {
                    @Override
                    public void run(Timeout timeout) {
                        onReceiveTimeoutFired(request);
                    }
                });

                Timeout timeout;
                try {
                    timeout = timer.newTimeout(receiveTimeoutTask, receiveTimeoutMs, TimeUnit.MILLISECONDS);
                }
                catch (IllegalStateException e) {
                    throw new TTransportException("Unable to schedule request timeout");
                }
                request.setReceiveTimeout(timeout);
            }
        }

        if (this.readTimeout != null) {
            long readTimeoutNanos = this.readTimeout.roundTo(TimeUnit.NANOSECONDS);
            if (readTimeoutNanos > 0) {
                TimerTask readTimeoutTask = new IoThreadBoundTimerTask(this, new ReadTimeoutTask(readTimeoutNanos, request));

                Timeout timeout;
                try {
                    timeout = timer.newTimeout(readTimeoutTask, readTimeoutNanos, TimeUnit.NANOSECONDS);
                }
                catch (IllegalStateException e) {
                    throw new TTransportException("Unable to schedule read timeout");
                }
                request.setReadTimeout(timeout);
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
        private final Listener listener;
        private Timeout sendTimeout;
        private Timeout receiveTimeout;

        private volatile Timeout readTimeout;

        public Request(Listener listener)
        {
            this.listener = listener;
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

        public Timeout getReadTimeout()
        {
            return readTimeout;
        }

        public void setReadTimeout(Timeout readTimeout)
        {
            this.readTimeout = readTimeout;
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

    private final class ReadTimeoutTask implements TimerTask
    {
        private final TimeoutHandler timeoutHandler;
        private final long timeoutNanos;
        private final Request request;

        ReadTimeoutTask(long timeoutNanos, Request request)
        {
            this.timeoutHandler = TimeoutHandler.findTimeoutHandler(getNettyChannel().pipeline());
            this.timeoutNanos = timeoutNanos;
            this.request = request;
        }

        public void run(Timeout timeout) throws Exception
        {
            if (timeoutHandler == null) {
                return;
            }

            if (timeout.isCancelled()) {
                return;
            }

            if (!getNettyChannel().isOpen()) {
                return;
            }

            long currentTimeNanos = System.nanoTime();

            long timePassed = currentTimeNanos - timeoutHandler.getLastMessageReceivedNanos();
            long nextDelayNanos =  timeoutNanos - timePassed;

            if (nextDelayNanos <= 0) {
                onReadTimeoutFired(request);
            }
            else {
                request.setReadTimeout(timer.newTimeout(this, nextDelayNanos, TimeUnit.NANOSECONDS));
            }
        }
    }
}
