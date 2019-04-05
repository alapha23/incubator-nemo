/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.bytetransfer;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Manages multiple transport contexts for one channel.
 */
final class ContextManager extends SimpleChannelInboundHandler<ByteTransferContextSetupMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(ContextManager.class.getName());

  private final PipeManagerWorker pipeManagerWorker;
  private final BlockManagerWorker blockManagerWorker;
  private final ByteTransfer byteTransfer;
  private final ChannelGroup channelGroup;
  private final String localExecutorId;
  private final Channel channel;
  private volatile String remoteExecutorId = null;

  private final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByLocal = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByLocal = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByRemote = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByRemote = new ConcurrentHashMap<>();
  private final AtomicInteger nextInputTransferIndex = new AtomicInteger(0);
  private final AtomicInteger nextOutputTransferIndex = new AtomicInteger(0);

  private final ScheduledExecutorService flusher;

  /**
   * Creates context manager for this channel.
   * @param pipeManagerWorker   provides handler for new contexts by remote executors
   * @param blockManagerWorker  provides handler for new contexts by remote executors
   * @param byteTransfer        provides channel caching
   * @param channelGroup        to cleanup this channel when closing {@link ByteTransport}
   * @param localExecutorId     local executor id
   * @param channel             the {@link Channel} to manage
   */
  ContextManager(final PipeManagerWorker pipeManagerWorker,
                 final BlockManagerWorker blockManagerWorker,
                 final ByteTransfer byteTransfer,
                 final ChannelGroup channelGroup,
                 final String localExecutorId,
                 final Channel channel) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.blockManagerWorker = blockManagerWorker;
    this.byteTransfer = byteTransfer;
    this.channelGroup = channelGroup;
    this.localExecutorId = localExecutorId;
    this.channel = channel;
    this.flusher = Executors.newSingleThreadScheduledExecutor();
    flusher.scheduleAtFixedRate(() -> {

      if (channel.isOpen()) {
        channel.flush();
      }

    }, 2, 2, TimeUnit.SECONDS);
  }


  /**
   * @return channel for this context manager.
   */
  Channel getChannel() {
    return channel;
  }

  /**
   * Returns {@link ByteInputContext} to provide {@link io.netty.buffer.ByteBuf}s on.
   * @param dataDirection the data direction
   * @param transferIndex transfer index
   * @return the {@link ByteInputContext} corresponding to the pair of {@code dataDirection} and {@code transferIndex}
   */
  ByteInputContext getInputContext(final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
                                   final int transferIndex) {
    final ConcurrentMap<Integer, ByteInputContext> contexts =
        dataDirection == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA
            ? inputContextsInitiatedByRemote : inputContextsInitiatedByLocal;
    return contexts.get(transferIndex);
  }

  /**
   * Responds to new transfer contexts by a remote executor.
   * @param ctx netty {@link ChannelHandlerContext}
   * @param message context setup message from the remote executor
   * @throws Exception exceptions from handler
   */
  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteTransferContextSetupMessage message)
      throws Exception {
    setRemoteExecutorId(message.getInitiatorExecutorId());
    byteTransfer.onNewContextByRemoteExecutor(message.getInitiatorExecutorId(), channel);
    final ByteTransferContextSetupMessage.ByteTransferDataDirection
      dataDirection = message.getDataDirection();
    final int transferIndex = message.getTransferIndex();
    final boolean isPipe = message.getIsPipe();
    final ByteTransferContext.ContextId contextId =
      new ByteTransferContext.ContextId(remoteExecutorId, localExecutorId, dataDirection, transferIndex, isPipe);
    final byte[] contextDescriptor = message.getContextDescriptor();

    if (message.getIsRestart()) {
      final ByteInputContext context = inputContextsInitiatedByRemote.get(transferIndex);
      LOG.info("Context restart !! {}, {}", contextId, context);

      if (isPipe) {
        pipeManagerWorker.onInputContext(context);
      }
    } else {

      if (dataDirection == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA) {
        final ByteInputContext context = inputContextsInitiatedByRemote.compute(transferIndex, (index, existing) -> {
          if (existing != null) {
            throw new RuntimeException(String.format("Duplicate ContextId: %s, transferIndex: %d", contextId,
              transferIndex));
          }
          return new StreamRemoteByteInputContext(remoteExecutorId, contextId, contextDescriptor, this);
        });

        if (isPipe) {
          pipeManagerWorker.onInputContext(context);
        } else {
          blockManagerWorker.onInputContext(context);
        }
      } else {
        final ByteOutputContext context = outputContextsInitiatedByRemote.compute(transferIndex, (idx, existing) -> {
          if (existing != null) {
            throw new RuntimeException(String.format("Duplicate ContextId: %s", contextId));
          }
          return new RemoteByteOutputContext(remoteExecutorId, contextId, contextDescriptor, this);
        });
        if (isPipe) {
          pipeManagerWorker.onOutputContext(context);
        } else {
          blockManagerWorker.onOutputContext(context);
        }
      }
    }
  }

  /**
   * Removes the specified contexts from map.
   * @param context the {@link ByteTransferContext} to remove.
   */
  void onContextExpired(final ByteTransferContext context) {
    final ByteTransferContext.ContextId contextId = context.getContextId();
    final ConcurrentMap<Integer, ? extends ByteTransferContext> contexts = context instanceof ByteInputContext
        ? (contextId.getDataDirection() == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA
            ? inputContextsInitiatedByRemote : inputContextsInitiatedByLocal)
        : (contextId.getDataDirection() == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA
            ? outputContextsInitiatedByLocal : outputContextsInitiatedByRemote);

    contexts.remove(contextId.getTransferIndex(), context);
  }

  public void onContextCloseLocal(
    final int transferIndex) {
    final ByteInputContext localInputContext = inputContextsInitiatedByRemote.get(transferIndex);
    inputContextsInitiatedByRemote.remove(transferIndex);
    localInputContext.onContextClose();
  }

  public void onContextRestartLocal(
    final int transferIndex) {
    final ByteInputContext localInputContext = inputContextsInitiatedByRemote.get(transferIndex);
    LOG.info("local context restart!! {}", localInputContext.getContextId());
    if (localInputContext.isFinished()) {
      LOG.info("Finish is not processed yet... we just set finish false: {}", localInputContext.getContextId());
      localInputContext.onContextRestart();
    } else {
      try {
        pipeManagerWorker.onInputContext(localInputContext);
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  public void onContextStopLocal(
    final int transferIndex) {
    final ByteInputContext localInputContext = inputContextsInitiatedByRemote.get(transferIndex);
    LOG.info("local context stop!! {}", localInputContext.getContextId());
    localInputContext.onContextStop();
  }

  void onContextStop(final ByteInputContext context) {
    LOG.info("context stop!! {}", context.getContextId());
    final ByteTransferContext.ContextId contextId = context.getContextId();
    inputContextsInitiatedByRemote.remove(contextId.getTransferIndex(), context);
    final ByteInputContext restartContext = new StreamRemoteByteInputContext(
      contextId.getInitiatorExecutorId(), contextId, context.getContextDescriptor(), this);
    inputContextsInitiatedByRemote.put(contextId.getTransferIndex(), restartContext);
  }

  /**
   * Initiates a context and stores to the specified map.
   * @param contexts map for storing context
   * @param transferIndexCounter counter for generating transfer index
   * @param dataDirection data direction to include in the context id
   * @param contextGenerator a function that returns context from context id
   * @param executorId id of the remote executor
   * @param <T> {@link ByteInputContext} or {@link ByteOutputContext}
   * @param isPipe is a pipe context
   * @return generated context
   */
  <T extends ByteTransferContext> T newContext(final ConcurrentMap<Integer, T> contexts,
                                               final AtomicInteger transferIndexCounter,
                                               final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
                                               final Function<ByteTransferContext.ContextId, T> contextGenerator,
                                               final String executorId,
                                               final boolean isPipe,
                                               final boolean isLocal) {
    setRemoteExecutorId(executorId);
    final int transferIndex = transferIndexCounter.getAndIncrement();
    final ByteTransferContext.ContextId contextId = new ByteTransferContext.ContextId(localExecutorId, executorId, dataDirection, transferIndex, isPipe);
    final T context = contexts.compute(transferIndex, (index, existingContext) -> {
      if (existingContext != null) {
        throw new RuntimeException(String.format("Duplicate ContextId: %s", contextId));
      }
      return contextGenerator.apply(contextId);
    });

    final ByteTransferContextSetupMessage message =
      new ByteTransferContextSetupMessage(localExecutorId,
        context.getContextId().getTransferIndex(),
        context.getContextId().getDataDirection(),
        context.getContextDescriptor(),
        context.getContextId().isPipe());

    if (isLocal) {
      // do nothing
    } else {
      channel.writeAndFlush(message).addListener(context.getChannelWriteListener());
    }
    return context;
  }

  /**
   * Create a new {@link ByteInputContext}.
   * @param executorId target executor id
   * @param contextDescriptor the context descriptor
   * @param isPipe            is pipe
   * @return new {@link ByteInputContext}
   */
  ByteInputContext newInputContext(final String executorId, final byte[] contextDescriptor, final boolean isPipe) {
    return newContext(inputContextsInitiatedByLocal, nextInputTransferIndex,
      ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA,
        contextId -> new StreamRemoteByteInputContext(executorId, contextId, contextDescriptor, this),
        executorId, isPipe, false);
  }

  /**
   * Create a new {@link ByteOutputContext}.
   * @param executorId target executor id
   * @param contextDescriptor the context descriptor
   * @param isPipe            is pipe
   * @return new {@link ByteOutputContext}
   */
  ByteOutputContext newOutputContext(final String executorId, final byte[] contextDescriptor, final boolean isPipe) {
    if (localExecutorId.equals(executorId)) {
      // TODO: create local output context
      final Queue<Object> queue = new ConcurrentLinkedQueue<>();
      return newContext(outputContextsInitiatedByLocal, nextOutputTransferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> {
        final LocalByteInputContext localByteInputContext =
          new LocalByteInputContext(executorId, contextId, contextDescriptor, this, queue);

          inputContextsInitiatedByRemote.put(contextId.getTransferIndex(), localByteInputContext);
          try {
            pipeManagerWorker.onInputContext(localByteInputContext);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          return new LocalByteOutputContext(executorId, contextId, contextDescriptor, this, queue);
        },
        executorId, isPipe, true);

    } else {
      return newContext(outputContextsInitiatedByLocal, nextOutputTransferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> new RemoteByteOutputContext(executorId, contextId, contextDescriptor, this),
        executorId, isPipe, false);
    }
  }

  /**
   * Set this contest manager as connected to the specified remote executor.
   * @param executorId the remote executor id
   */
  private void setRemoteExecutorId(final String executorId) {
    if (remoteExecutorId == null) {
      remoteExecutorId = executorId;
    } else if (!executorId.equals(remoteExecutorId)) {
      throw new RuntimeException(String.format("Wrong ContextManager: (%s != %s)", executorId, remoteExecutorId));
    }
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    channelGroup.add(ctx.channel());
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    channelGroup.remove(ctx.channel());
    final Throwable cause = new Exception("Channel closed");
    throwChannelErrorOnContexts(inputContextsInitiatedByLocal, cause);
    throwChannelErrorOnContexts(outputContextsInitiatedByLocal, cause);
    throwChannelErrorOnContexts(inputContextsInitiatedByRemote, cause);
    throwChannelErrorOnContexts(outputContextsInitiatedByRemote, cause);
  }

  /**
   * Invoke {@link ByteTransferContext#onChannelError(Throwable)} on the specified contexts.
   * @param contexts map storing the contexts
   * @param cause the error
   * @param <T> {@link ByteInputContext} or {@link ByteOutputContext}
   */
  private <T extends ByteTransferContext> void throwChannelErrorOnContexts(final ConcurrentMap<Integer, T> contexts,
                                                                           final Throwable cause) {
    for (final ByteTransferContext context : contexts.values()) {
      context.onChannelError(cause);
    }
  }
}
