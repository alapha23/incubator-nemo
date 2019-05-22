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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import org.apache.beam.vendor.protobuf.v3.com.google.protobuf.ByteOutput;
import org.apache.nemo.common.NemoTriple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.TaskLocationMap;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.datatransfer.VMScalingClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.nemo.runtime.executor.common.TaskLocationMap.LOC.SF;
import static org.apache.nemo.runtime.executor.common.TaskLocationMap.LOC.VM;

/**
 * Manages multiple transport contexts for one channel.
 */
final class DefaultContextManagerImpl extends SimpleChannelInboundHandler<ByteTransferContextSetupMessage> implements ContextManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultContextManagerImpl.class.getName());

  private final PipeManagerWorker pipeManagerWorker;
  private final BlockManagerWorker blockManagerWorker;
  private final ByteTransfer byteTransfer;
  private final ChannelGroup channelGroup;
  private final String localExecutorId;
  private final Channel channel;
  private volatile String remoteExecutorId = null;

  private final ConcurrentMap<Integer, ByteInputContext> inputContexts;
  private final ConcurrentMap<Integer, ByteOutputContext> outputContexts;
  //private final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByRemote;
  //private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByRemote;
  private final AtomicInteger nextInputTransferIndex;
  private final AtomicInteger nextOutputTransferIndex;

  private final ScheduledExecutorService flusher;
  private final VMScalingClientTransport vmScalingClientTransport;
  private final AckScheduledService ackScheduledService;

  // key: runtimeId, taskIndex, outputStream , value: transferIndex
  private final Map<TransferKey, Integer> taskTransferIndexMap;

  private final TaskLocationMap taskLocationMap;

  private final ExecutorService channelExecutorService;
  /**
   * Creates context manager for this channel.
   * @param pipeManagerWorker   provides handler for new contexts by remote executors
   * @param blockManagerWorker  provides handler for new contexts by remote executors
   * @param byteTransfer        provides channel caching
   * @param channelGroup        to cleanup this channel when closing {@link ByteTransport}
   * @param localExecutorId     local executor id
   * @param channel             the {@link Channel} to manage
   */
  DefaultContextManagerImpl(final ExecutorService channelExecutorService,
                            final PipeManagerWorker pipeManagerWorker,
                            final BlockManagerWorker blockManagerWorker,
                            final ByteTransfer byteTransfer,
                            final ChannelGroup channelGroup,
                            final String localExecutorId,
                            final Channel channel,
                            final VMScalingClientTransport vmScalingClientTransport,
                            final AckScheduledService ackScheduledService,
                            final Map<TransferKey, Integer> taskTransferIndexMap,
                            final ConcurrentMap<Integer, ByteInputContext> inputContexts,
                            final ConcurrentMap<Integer, ByteOutputContext> outputContexts,
                            //final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByRemote,
                            //final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByRemote,
                            final AtomicInteger nextInputTransferIndex,
                            final AtomicInteger nextOutputTransferIndex,
                            final TaskLocationMap taskLocationMap) {
    this.channelExecutorService = channelExecutorService;
    this.pipeManagerWorker = pipeManagerWorker;
    this.blockManagerWorker = blockManagerWorker;
    this.byteTransfer = byteTransfer;
    this.channelGroup = channelGroup;
    this.localExecutorId = localExecutorId;
    this.vmScalingClientTransport = vmScalingClientTransport;
    this.ackScheduledService = ackScheduledService;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.channel = channel;
    this.flusher = Executors.newSingleThreadScheduledExecutor();
    this.inputContexts = inputContexts;
    //this.inputContextsInitiatedByLocal = inputContextsInitiatedByLocal;
    this.outputContexts = outputContexts;
    //this.outputContextsInitiatedByLocal = outputContextsInitiatedByLocal;
    this.nextInputTransferIndex = nextInputTransferIndex;
    this.nextOutputTransferIndex = nextOutputTransferIndex;
    this.taskLocationMap = taskLocationMap;
    flusher.scheduleAtFixedRate(() -> {

      if (channel.isOpen()) {
        channel.flush();
      }

    }, 2, 2, TimeUnit.SECONDS);
  }


  /**
   * @return channel for this context manager.
   */
  @Override
  public Channel getChannel() {
    return channel;
  }

  @Override
  public boolean isRelayServerContext() {
    return false;
  }

  /**
   * Returns {@link ByteInputContext} to provide {@link io.netty.buffer.ByteBuf}s on.
   * @param dataDirection the data direction
   * @param transferIndex transfer index
   * @return the {@link ByteInputContext} corresponding to the pair of {@code dataDirection} and {@code transferIndex}
   */
  @Override
  public ByteInputContext getInputContext(final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
                                   final int transferIndex) {
    //LOG.info("Get input context: {} / {}", transferIndex, inputContextsInitiatedByRemote);
    final ConcurrentMap<Integer, ByteInputContext> contexts =
        dataDirection == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA
            ? inputContexts : inputContexts;
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

    switch (message.getMessageType()) {
      case ACK_FROM_UPSTREAM: {
        final ByteInputContext context = inputContexts.get(transferIndex);
        LOG.info("ACK_FROM_UPSTREAM: {}, {}", transferIndex, inputContexts);
        context.receivePendingAck();
        break;
      }
      case ACK_FROM_DOWNSTREAM: {
        final ByteOutputContext context = outputContexts.get(transferIndex);
        LOG.info("ACK_FROM_DOWNSTREAM: {}, {}", transferIndex, context);
        context.receivePendingAck();
        break;
      }
      case PENDING_FOR_SCALEOUT_VM: {
        // this means that the downstream task will be moved to another machine
        // so we should stop sending data to the downstream task
        final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);

        // It means that the remote dst task is moved to SF (or new VM)
        taskLocationMap.locationMap.put(new NemoTriple<>(cd.getRuntimeEdgeId(), (int) cd.getDstTaskIndex(), true), SF);

        LOG.info("SCALEOUT pending {}, {}", transferIndex, outputContexts);
        final ByteOutputContext outputContext = outputContexts.get(transferIndex);
        outputContext.pending(ByteOutputContext.SendDataTo.SF, null, 1);
        break;
      }
      case PENDING_FOR_SCALEIN_VM: {
        final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);
        // It means that the remote dst task is moved to the origin VM
        taskLocationMap.locationMap.put(new NemoTriple<>(cd.getRuntimeEdgeId(), (int) cd.getDstTaskIndex(), true), VM);

        LOG.info("SCALEIN pending {}", transferIndex);
        final ByteOutputContext outputContext = outputContexts.get(transferIndex);
        outputContext.pending(ByteOutputContext.SendDataTo.VM, null, 1);
        break;
      }
      case STOP_INPUT_FOR_SCALEOUT: {
        final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);
        LOG.info("Scaling out input context {} to SF", Pair.of(cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex()));
        taskLocationMap.locationMap.put(new NemoTriple<>(cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex(), false), SF);
        channelExecutorService.execute(() -> {
          LOG.info("Sending ack to the input context");
          final ByteTransferContextSetupMessage ackMessage =
            new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
              contextId.getTransferIndex(),
              contextId.getDataDirection(),
              contextDescriptor,
              contextId.isPipe(),
              ByteTransferContextSetupMessage.MessageType.ACK_FROM_DOWNSTREAM);
          channel.writeAndFlush(ackMessage);
        });
        break;
      }
      case STOP_INPUT_FOR_SCALEIN: {
        final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);
        LOG.info("Scaling in input context {} to VM", Pair.of(cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex()));
        taskLocationMap.locationMap.put(new NemoTriple<>(cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex(), false), VM);
        break;
      }
      case RESUME_AFTER_SCALEOUT_VM: {
        throw new RuntimeException("Not supported!!");
        /*
        final String address = message.getMovedAddress();
        final String taskId = message.getTaskId();
        LOG.info("Resume {} to {}/{}", transferIndex, address, taskId);
        final ByteOutputContext outputContext = outputContexts.get(transferIndex);
        outputContext.scaleoutToVm(address, taskId);
        break;
        */
      }
      case RESUME_AFTER_SCALEIN_VM: {
        LOG.info("Resume scaling {}", transferIndex);
        final ByteOutputContext outputContext = outputContexts.get(transferIndex);
        outputContext.scaleInToVm();
        break;
      }
      case RESTART: {
        final ByteInputContext context = inputContexts.get(transferIndex);
        LOG.info("Context restart !! {}, {}", contextId, context);

        if (isPipe) {
          pipeManagerWorker.onInputContext(context);
        }
        break;
      }
      case CONTROL: {

        if (dataDirection == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA) {
          LOG.info("inputContextsInitiatedByRemote: {}", inputContexts);

          LOG.info("Input Receive transfer index : {}", transferIndex);
          if (inputContexts.containsKey(transferIndex)) {
            LOG.warn("Duplicate input context ContextId: {}, transferIndex: {} due to the remote channel", contextId,
                transferIndex);
            final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);

            // THIS always should be in SF
            taskLocationMap.locationMap.put(
              new NemoTriple<>(cd.getRuntimeEdgeId(), (int)cd.getSrcTaskIndex(), false), TaskLocationMap.LOC.SF);

            //return inputContextsInitiatedByRemote.get(transferIndex);
          } else {
            final ByteInputContext c = new StreamRemoteByteInputContext(
              remoteExecutorId, contextId, contextDescriptor, this, ackScheduledService.ackService);

            // ADD Task Transfer Index !!
            final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);
            final TransferKey key =
              new TransferKey(cd.getRuntimeEdgeId(),
                (int) cd.getSrcTaskIndex(), (int) cd.getDstTaskIndex(), false);

            // This always should be VM
            taskLocationMap.locationMap.put(
              new NemoTriple<>(cd.getRuntimeEdgeId(), (int)cd.getSrcTaskIndex(), false), VM);


            taskTransferIndexMap.put(key, transferIndex);

            if (isPipe) {
              try {
                pipeManagerWorker.onInputContext(c);
              } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
            } else {
              blockManagerWorker.onInputContext(c);
            }
            inputContexts.putIfAbsent(transferIndex, c);
          }

        } else {
          LOG.info("outputContextsInitiatedByRemote: {}", outputContexts);
          LOG.info("Output Receive transfer index : {}", transferIndex);
          if (outputContexts.containsKey(transferIndex)) {
            LOG.warn("Duplicate output context ContextId: {}, transferIndex: {} due to the remote channel", contextId,
              transferIndex);
            final String addr = ctx.channel().remoteAddress().toString().split(":")[0];
            LOG.info("Remote byte output address: {} ", addr);
            final ByteOutputContext outputContext = outputContexts.get(transferIndex);
            outputContext.scaleoutToVm(channel);
          } else {
            final ByteOutputContext c = new RemoteByteOutputContext(remoteExecutorId, contextId,
              contextDescriptor, this);
            try {
              if (isPipe) {
                pipeManagerWorker.onOutputContext(c);
              } else {
                blockManagerWorker.onOutputContext(c);
              }
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            outputContexts.putIfAbsent(transferIndex, c);
          }
        }
        break;
      }
      default:
        throw new RuntimeException("Unsupported type: " + message.getMessageType());
    }
  }

  /**
   * Removes the specified contexts from map.
   * @param context the {@link ByteTransferContext} to remove.
   */
  @Override
  public void onContextExpired(final ByteTransferContext context) {
    final ByteTransferContext.ContextId contextId = context.getContextId();
    final ConcurrentMap<Integer, ? extends ByteTransferContext> contexts = context instanceof ByteInputContext
        ? (contextId.getDataDirection() == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA
            ? inputContexts : inputContexts)
        : (contextId.getDataDirection() == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA
            ? outputContexts : outputContexts);

    contexts.remove(contextId.getTransferIndex(), context);
  }

  @Override
  public void onContextCloseLocal(
    final int transferIndex) {
    final ByteInputContext localInputContext = inputContexts.get(transferIndex);
    inputContexts.remove(transferIndex);
    localInputContext.onContextClose();
  }

  @Override
  public void onContextRestartLocal(
    final int transferIndex) {
    // REMOVE!!!

    final ByteInputContext localInputContext = inputContexts.get(transferIndex);
    LOG.info("local context restart!! {}", localInputContext.getContextId());
    final ByteInputContext localByteInputContext = new LocalByteInputContext(
      localInputContext.getRemoteExecutorId(),
      localInputContext.getContextId(),
      localInputContext.getContextDescriptor(),
      this,
      ((LocalByteInputContext)localInputContext).getQueue(),
      ((LocalByteInputContext)localInputContext).getLocalByteOutputContext(),
      ackScheduledService.ackService);

    inputContexts.remove(transferIndex);
    inputContexts.put(transferIndex, localByteInputContext);

    try {
      pipeManagerWorker.onInputContext(localByteInputContext);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onContextStopLocal(
    final int transferIndex) {
    final ByteInputContext localInputContext = inputContexts.get(transferIndex);
    LOG.info("local context stop!! {}", localInputContext.getContextId());
    localInputContext.onContextStop();
  }

  @Override
  public void onContextStop(final ByteInputContext context) {
    LOG.info("context stop!! {}", context.getContextId());
    final ByteTransferContext.ContextId contextId = context.getContextId();
    inputContexts.remove(contextId.getTransferIndex(), context);
    final ByteInputContext restartContext = new StreamRemoteByteInputContext(
      contextId.getInitiatorExecutorId(), contextId, context.getContextDescriptor(), this, ackScheduledService.ackService);
    inputContexts.put(contextId.getTransferIndex(), restartContext);
  }

  /**
   * Initiates a context and stores to the specified map.
   * @param contexts map for storing context
   * @param dataDirection data direction to include in the context id
   * @param contextGenerator a function that returns context from context id
   * @param executorId id of the remote executor
   * @param <T> {@link ByteInputContext} or {@link ByteOutputContext}
   * @param isPipe is a pipe context
   * @return generated context
   */
  <T extends ByteTransferContext> T newContext(final ConcurrentMap<Integer, T> contexts,
                                               final int transferIndex,
                                               final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
                                               final Function<ByteTransferContext.ContextId, T> contextGenerator,
                                               final String executorId,
                                               final boolean isPipe,
                                               final boolean isLocal) {
    setRemoteExecutorId(executorId);
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
  @Override
  public ByteInputContext newInputContext(final String executorId, final byte[] contextDescriptor, final boolean isPipe) {
    final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);
    final TransferKey key = new TransferKey(cd.getRuntimeEdgeId(),
      (int) cd.getSrcTaskIndex(), (int) cd.getDstTaskIndex(), false);

    final int transferIndex = nextInputTransferIndex.getAndIncrement();
    taskTransferIndexMap.put(key, transferIndex);

    return newContext(inputContexts, transferIndex,
      ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA,
        contextId -> new StreamRemoteByteInputContext(executorId, contextId, contextDescriptor, this, ackScheduledService.ackService),
        executorId, isPipe, false);
  }

  /**
   * Create a new {@link ByteOutputContext}.
   * @param executorId target executor id
   * @param isPipe            is pipe
   * @return new {@link ByteOutputContext}
   */
  @Override
  public ByteOutputContext newOutputContext(final String executorId,
                                            final PipeTransferContextDescriptor descriptor,
                                            final boolean isPipe) {
    final TransferKey key = new TransferKey(descriptor.getRuntimeEdgeId(),
      (int) descriptor.getSrcTaskIndex(), (int) descriptor.getDstTaskIndex(), true);

    final int transferIndex = nextOutputTransferIndex.getAndIncrement();
    taskTransferIndexMap.put(key, transferIndex);

    // FIRST initiation should be in VM
    taskLocationMap.locationMap.put(new NemoTriple<>(descriptor.getRuntimeEdgeId(),
      (int) descriptor.getDstTaskIndex(), true), VM);

     return newContext(outputContexts, transferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> new RemoteByteOutputContext(executorId, contextId,
          descriptor.encode(), this),
        executorId, isPipe, false);

    /*
    if (localExecutorId.equals(executorId)) {
      final Queue<Object> queue = new ConcurrentLinkedQueue<>();
      return newContext(outputContextsInitiatedByLocal, transferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> {
        final LocalByteOutputContext localByteOutputContext =
          new LocalByteOutputContext(executorId, contextId,
            contextDescriptor, this, queue, vmScalingClientTransport);

        final LocalByteInputContext localByteInputContext =
          new LocalByteInputContext(executorId, contextId, contextDescriptor, this, queue, localByteOutputContext, ackScheduledService.ackService);

        localByteOutputContext.setLocalByteInputContext(localByteInputContext);

          inputContextsInitiatedByRemote.put(contextId.getTransferIndex(), localByteInputContext);
          try {
            pipeManagerWorker.onInputContext(localByteInputContext);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          return localByteOutputContext;
        },
        executorId, isPipe, true);

    } else {
      return newContext(outputContextsInitiatedByLocal, transferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> new RemoteByteOutputContext(executorId, contextId,
          contextDescriptor, this, vmScalingClientTransport),
        executorId, isPipe, false);
    }
    */
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
    throwChannelErrorOnContexts(inputContexts, cause);
    throwChannelErrorOnContexts(outputContexts, cause);
    //throwChannelErrorOnContexts(inputContextsInitiatedByRemote, cause);
    //throwChannelErrorOnContexts(outputContextsInitiatedByRemote, cause);
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
