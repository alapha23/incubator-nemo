package org.apache.nemo.runtime.executor.offloading.lambda;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.OffloadingWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public final class LambdaWorkerProxy implements OffloadingWorker {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaWorkerProxy.class.getName());
  private final Channel channel;
  private final BlockingQueue resultQueue;
  private final BlockingQueue<NemoEvent> endQueue;
  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelEventHandlerMap;

  private final EncoderFactory inputEncoderFactory;
  private final DecoderFactory inputDecoderFactory;

  private final EncoderFactory outputEncoderFactory;
  private final DecoderFactory outputDecoderFactory;

  private ByteBufOutputStream byteBufOutputStream;

  private EncoderFactory.Encoder encoder;

  public LambdaWorkerProxy(final Channel channel,
                           final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelEventHandlerMap,
                           final EncoderFactory inputEncoderFactory,
                           final DecoderFactory inputDecoderFactory,
                           final EncoderFactory outputEncoderFactory,
                           final DecoderFactory outputDecoderFactory) {
    this.channel = channel;
    this.byteBufOutputStream = new ByteBufOutputStream(channel.alloc().ioBuffer());
    this.channelEventHandlerMap = channelEventHandlerMap;
    this.resultQueue = new LinkedBlockingQueue<>();
    this.endQueue = new LinkedBlockingQueue<>();

    channelEventHandlerMap.put(channel, new EventHandler<NemoEvent>() {
      @Override
      public void onNext(NemoEvent msg) {
        switch (msg.getType()) {
          case RESULT:
            LOG.info("Receive result");
            final ByteBufInputStream bis = new ByteBufInputStream(msg.getByteBuf());
            try {
              final DecoderFactory.Decoder decoder = outputDecoderFactory.create(bis);
              final Object data = decoder.decode();
              resultQueue.add(data);
              msg.getByteBuf().release();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException();
            }
            break;
          case END:
            LOG.info("Receive end");
            msg.getByteBuf().release();
            endQueue.add(msg);
            break;
          default:
            throw new RuntimeException("Invalid type: " + msg);
        }
      }
    });

    this.inputEncoderFactory = inputEncoderFactory;
    this.inputDecoderFactory = inputDecoderFactory;
    this.outputEncoderFactory = outputEncoderFactory;
    this.outputDecoderFactory = outputDecoderFactory;

    try {
      byteBufOutputStream.writeInt(NemoEvent.Type.DATA.ordinal());
      this.encoder = inputEncoderFactory.create(byteBufOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> List<T> getResult() {
    //LOG.info("Get result");
    final List<T> result = new ArrayList<>();

    while (endQueue.peek() != null) {
      while (!resultQueue.isEmpty()) {
        result.add((T) resultQueue.poll());
      }

      //LOG.info("Result is empty, but don't receive end message");

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    //LOG.info("We received end message!");

    while (!resultQueue.isEmpty()) {
      result.add((T) resultQueue.poll());
    }

    return result;
  }

  @Override
  public void write(final Object input) {
    try {
      encoder.encode(input);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() {
    try {
      byteBufOutputStream.close();

      //LOG.info("Flush data");
      channel.writeAndFlush(new NemoEvent(NemoEvent.Type.DATA, byteBufOutputStream.buffer()));

      byteBufOutputStream = new ByteBufOutputStream(channel.alloc().ioBuffer());
      byteBufOutputStream.writeInt(NemoEvent.Type.DATA.ordinal());
      encoder = inputEncoderFactory.create(byteBufOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finishOffloading() {
    channel.writeAndFlush(new NemoEvent(NemoEvent.Type.END, new byte[0], 0));
  }
}