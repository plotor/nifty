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

import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.nifty.duplex.TProtocolPair;
import com.facebook.nifty.duplex.TTransportPair;
import com.facebook.nifty.processor.NiftyProcessorFactory;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.Timer;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dispatch TNiftyTransport to the TProcessor and write output back.
 *
 * Note that all current async thrift clients are capable of sending multiple requests at once
 * but not capable of handling out-of-order responses to those requests, so this dispatcher
 * sends the requests in order. (Eventually this will be conditional on a flag in the thrift
 * message header for future async clients that can handle out-of-order responses).
 */
public class NiftyDispatcher extends ChannelInboundHandlerAdapter {

    private final NiftyProcessorFactory processorFactory;
    private final Executor executor;
    private final long taskTimeoutMillis;
    private final Timer taskTimeoutTimer;
    private final int queuedResponseLimit;
    private final Map<Integer, ThriftMessage> responseMap = new HashMap<>();
    private final AtomicInteger dispatcherSequenceId = new AtomicInteger(0);
    private final AtomicInteger lastResponseWrittenId = new AtomicInteger(0);
    private final TDuplexProtocolFactory duplexProtocolFactory;

    public NiftyDispatcher(ThriftServerDef def, Timer timer) {
        this.processorFactory = def.getProcessorFactory();
        this.duplexProtocolFactory = def.getDuplexProtocolFactory();
        this.queuedResponseLimit = def.getQueuedResponseLimit();
        this.executor = def.getExecutor();
        this.taskTimeoutMillis = (def.getTaskTimeout() == null ? 0 : def.getTaskTimeout().toMillis());
        this.taskTimeoutTimer = (def.getTaskTimeout() == null ? null : timer);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        if (message instanceof ThriftMessage) {
            ThriftMessage thriftMessage = (ThriftMessage) message;
            if (taskTimeoutMillis > 0) {
                thriftMessage.setProcessStartTimeMillis(System.currentTimeMillis());
            }
            this.checkResponseOrderingRequirements(ctx, thriftMessage);

            TNiftyTransport messageTransport = new TNiftyTransport(ctx.channel(), thriftMessage);
            TTransportPair transportPair = TTransportPair.fromSingleTransport(messageTransport);
            TProtocolPair protocolPair = duplexProtocolFactory.getProtocolPair(transportPair);
            TProtocol inProtocol = protocolPair.getInputProtocol();
            TProtocol outProtocol = protocolPair.getOutputProtocol();

            try {
                this.processRequest(ctx, thriftMessage, messageTransport, inProtocol, outProtocol);
            } catch (RejectedExecutionException ex) {
                TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR, "Server overloaded");
                this.sendTApplicationException(x, ctx, thriftMessage, messageTransport, inProtocol, outProtocol);
            }
        } else {
            super.channelRead(ctx, message);
        }
    }

    private void checkResponseOrderingRequirements(ChannelHandlerContext ctx, ThriftMessage message) {
        boolean messageRequiresOrderedResponses = message.isOrderedResponsesRequired();

        if (!DispatcherContext.isResponseOrderingRequirementInitialized(ctx)) {
            // This is the first request. This message will decide whether all responses on the
            // channel must be strictly ordered, or whether out-of-order is allowed.
            DispatcherContext.setResponseOrderingRequired(ctx, messageRequiresOrderedResponses);
        } else {
            // This is not the first request. Verify that the ordering requirement on this message
            // is consistent with the requirement on the channel itself.
            checkState(
                    messageRequiresOrderedResponses == DispatcherContext.isResponseOrderingRequired(ctx),
                    "Every message on a single channel must specify the same requirement for response ordering");
        }
    }

    private void processRequest(
            final ChannelHandlerContext ctx,
            final ThriftMessage message,
            final TNiftyTransport messageTransport,
            final TProtocol inProtocol,
            final TProtocol outProtocol) {
        // Remember the ordering of requests as they arrive, used to enforce an order on the responses.
        final int requestSequenceId = dispatcherSequenceId.incrementAndGet();

        synchronized (responseMap) {
            // Limit the number of pending responses (responses which finished out of order, and are
            // waiting for previous requests to be finished so they can be written in order), by
            // blocking further channel reads. Due to the way Netty frame decoders work, this is more
            // of an estimate than a hard limit. Netty may continue to decode and process several
            // more requests that were in the latest read, even while further reads on the channel
            // have been blocked.
            if (requestSequenceId > lastResponseWrittenId.get() + queuedResponseLimit &&
                    !DispatcherContext.isChannelReadBlocked(ctx)) {
                DispatcherContext.blockChannelReads(ctx);
            }
        }

        executor.execute(() -> {
            ListenableFuture<Boolean> processFuture;
            final AtomicBoolean responseSent = new AtomicBoolean(false);

            try {
                try {
                    long timeRemaining = 0;
                    if (taskTimeoutMillis > 0) {
                        long timeElapsed = System.currentTimeMillis() - message.getProcessStartTimeMillis();
                        if (timeElapsed >= taskTimeoutMillis) {
                            TApplicationException taskTimeoutException = new TApplicationException(
                                    TApplicationException.INTERNAL_ERROR,
                                    "Task stayed on the queue for " + timeElapsed +
                                            " milliseconds, exceeding configured task timeout of " + taskTimeoutMillis + " milliseconds.");
                            this.sendTApplicationException(taskTimeoutException, ctx, message, messageTransport, inProtocol, outProtocol);
                            return;
                        } else {
                            timeRemaining = taskTimeoutMillis - timeElapsed;
                        }
                    }

                    if (timeRemaining > 0) {
                        taskTimeoutTimer.newTimeout(timeout -> {
                            // The immediateFuture returned by processors isn't cancellable, cancel() and
                            // isCanceled() always return false. Use a flag to detect task expiration.
                            if (responseSent.compareAndSet(false, true)) {
                                TApplicationException ex = new TApplicationException(
                                        TApplicationException.INTERNAL_ERROR, "Task timed out while executing."
                                );
                                // Create a temporary transport to send the exception
                                ByteBuf duplicateBuffer = message.getBuffer().duplicate();
                                duplicateBuffer.resetReaderIndex();
                                TNiftyTransport temporaryTransport = new TNiftyTransport(
                                        ctx.channel(),
                                        duplicateBuffer,
                                        message.getTransportType());
                                TProtocolPair protocolPair = duplexProtocolFactory.getProtocolPair(
                                        TTransportPair.fromSingleTransport(temporaryTransport));
                                this.sendTApplicationException(ex, ctx, message, temporaryTransport,
                                        protocolPair.getInputProtocol(),
                                        protocolPair.getOutputProtocol());
                            }
                        }, timeRemaining, TimeUnit.MILLISECONDS);
                    }

                    ConnectionContext connectionContext = ConnectionContexts.getContext(ctx.channel());
                    RequestContext requestContext = new NiftyRequestContext(connectionContext, inProtocol, outProtocol, messageTransport);
                    RequestContexts.setCurrentContext(requestContext);
                    processFuture = processorFactory.getProcessor(messageTransport).process(inProtocol, outProtocol, requestContext);
                } finally {
                    // RequestContext does NOT stay set while we are waiting for the process
                    // future to complete. This is by design because we'll might move on to the
                    // next request using this thread before this one is completed. If you need
                    // the context throughout an asynchronous handler, you need to read and store
                    // it before returning a future.
                    RequestContexts.clearCurrentContext();
                }

                Futures.addCallback(
                        processFuture,
                        new FutureCallback<Boolean>() {
                            @Override
                            public void onSuccess(Boolean result) {
                                try {
                                    // Only write if the task hasn't expired.
                                    if (responseSent.compareAndSet(false, true)) {
                                        ThriftMessage response = message.getMessageFactory().create(messageTransport.getOutputBuffer());
                                        NiftyDispatcher.this.writeResponse(ctx, response, requestSequenceId, DispatcherContext.isResponseOrderingRequired(ctx));
                                    }
                                } catch (Throwable t) {
                                    NiftyDispatcher.this.onDispatchException(ctx, t);
                                }
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                NiftyDispatcher.this.onDispatchException(ctx, t);
                            }
                        });
            } catch (TException e) {
                NiftyDispatcher.this.onDispatchException(ctx, e);
            }
        });
    }

    private void sendTApplicationException(
            TApplicationException x,
            ChannelHandlerContext ctx,
            ThriftMessage request,
            TNiftyTransport requestTransport,
            TProtocol inProtocol,
            TProtocol outProtocol) {
        // Don't serialize a TApplicationException if the client is no longer connected.
        //
        // Technically this check is redundant since we'll also check it in writeResponse, but this
        // may avoid wasting time building a serialized exception buffer when we aren't going to be
        // able to write it.

        if (ctx.channel().isActive()) {
            try {
                TMessage message = inProtocol.readMessageBegin();
                outProtocol.writeMessageBegin(new TMessage(message.name, TMessageType.EXCEPTION, message.seqid));
                x.write(outProtocol);
                outProtocol.writeMessageEnd();
                outProtocol.getTransport().flush();

                ThriftMessage response = request.getMessageFactory().create(requestTransport.getOutputBuffer());
                this.writeResponse(ctx, response, message.seqid, DispatcherContext.isResponseOrderingRequired(ctx));
            } catch (TException ex) {
                this.onDispatchException(ctx, ex);
            }
        }
    }

    private void onDispatchException(ChannelHandlerContext ctx, Throwable t) {
        ctx.fireExceptionCaught(t);
        this.closeChannel(ctx);
    }

    private void writeResponse(ChannelHandlerContext ctx,
                               ThriftMessage response,
                               int responseSequenceId,
                               boolean isOrderedResponsesRequired) {
        // Only write response if the client is still there.
        if (ctx.channel().isActive()) {
            if (isOrderedResponsesRequired) {
                this.writeResponseInOrder(ctx, response, responseSequenceId);
            } else {
                // No ordering required, just write the response immediately
                ctx.channel().write(response);
                lastResponseWrittenId.incrementAndGet();
            }
        }
    }

    private void writeResponseInOrder(ChannelHandlerContext ctx,
                                      ThriftMessage response,
                                      int responseSequenceId) {
        // Ensure responses to requests are written in the same order the requests
        // were received.
        synchronized (responseMap) {
            int currentResponseId = lastResponseWrittenId.get() + 1;
            if (responseSequenceId != currentResponseId) {
                // This response is NOT next in line of ordered responses, save it to
                // be sent later, after responses to all earlier requests have been
                // sent.
                responseMap.put(responseSequenceId, response);
            } else {
                // This response was next in line, write this response now, and see if
                // there are others next in line that should be sent now as well.
                do {
                    ctx.channel().write(response);
                    lastResponseWrittenId.incrementAndGet();
                    ++currentResponseId;
                    response = responseMap.remove(currentResponseId);
                } while (null != response);

                ctx.flush();

                // Now that we've written some responses, check if reads should be unblocked
                if (DispatcherContext.isChannelReadBlocked(ctx)) {
                    int lastRequestSequenceId = dispatcherSequenceId.get();
                    if (lastRequestSequenceId <= lastResponseWrittenId.get() + queuedResponseLimit) {
                        DispatcherContext.unblockChannelReads(ctx);
                    }
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // Any out of band exception are caught here and we tear down the socket
        this.closeChannel(ctx);

        // Send for logging
        super.exceptionCaught(ctx, cause);
    }

    private void closeChannel(ChannelHandlerContext ctx) {
        if (ctx.channel().isOpen()) {
            ctx.channel().close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Reads always start out unblocked
        DispatcherContext.unblockChannelReads(ctx);
        super.channelActive(ctx);
    }

    private static class DispatcherContext {
        private static final AttributeKey<DispatcherContext> DISPATCHER_CONTEXT = AttributeKey.valueOf("Nifty.DispatcherContext");

        private ReadBlockedState readBlockedState = ReadBlockedState.NOT_BLOCKED;
        private boolean responseOrderingRequired = false;
        private boolean responseOrderingRequirementInitialized = false;

        public static boolean isChannelReadBlocked(ChannelHandlerContext ctx) {
            return getDispatcherContext(ctx).readBlockedState == ReadBlockedState.BLOCKED;
        }

        public static void blockChannelReads(ChannelHandlerContext ctx) {
            // Remember that reads are blocked (there is no Channel.getReadable())
            getDispatcherContext(ctx).readBlockedState = ReadBlockedState.BLOCKED;

            // NOTE: this shuts down reads, but isn't a 100% guarantee we won't get any more messages.
            // It sets up the channel so that the polling loop will not report any new read events
            // and netty won't read any more data from the socket, but any messages already fully read
            // from the socket before this ran may still be decoded and arrive at this handler. Thus
            // the limit on queued messages before we block reads is more of a guidance than a hard
            // limit.
            // TODO(NETTY4): figure out how to handle this
            //ctx.channel().setReadable(false);
        }

        public static void unblockChannelReads(ChannelHandlerContext ctx) {
            // Remember that reads are unblocked (there is no Channel.getReadable())
            getDispatcherContext(ctx).readBlockedState = ReadBlockedState.NOT_BLOCKED;
            // TODO(NETTY4): figure out how to handle this
            //ctx.channel().setReadable(true);
        }

        public static void setResponseOrderingRequired(ChannelHandlerContext ctx, boolean required) {
            DispatcherContext dispatcherContext = getDispatcherContext(ctx);
            dispatcherContext.responseOrderingRequirementInitialized = true;
            dispatcherContext.responseOrderingRequired = required;
        }

        public static boolean isResponseOrderingRequired(ChannelHandlerContext ctx) {
            return getDispatcherContext(ctx).responseOrderingRequired;
        }

        public static boolean isResponseOrderingRequirementInitialized(ChannelHandlerContext ctx) {
            return getDispatcherContext(ctx).responseOrderingRequirementInitialized;
        }

        private static DispatcherContext getDispatcherContext(ChannelHandlerContext ctx) {
            DispatcherContext dispatcherContext = ctx.attr(DISPATCHER_CONTEXT).get();

            if (dispatcherContext == null) {
                // No context was added yet, add one
                dispatcherContext = new DispatcherContext();
                ctx.attr(DISPATCHER_CONTEXT).set(dispatcherContext);
            }

            return dispatcherContext;
        }

        private enum ReadBlockedState {
            NOT_BLOCKED,
            BLOCKED,
        }
    }
}
