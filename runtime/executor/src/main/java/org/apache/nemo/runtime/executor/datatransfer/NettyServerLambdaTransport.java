package org.apache.nemo.runtime.executor.datatransfer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.NettyChannelInitializer;
import org.apache.nemo.common.Pair;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class NettyServerLambdaTransport {
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;
  private static final String CLASS_NAME = NettyServerLambdaTransport.class.getName();
  private static final String ADDRESS = "172.31.6.35";
  private static final String PUBLIC_ADDRESS = "18.182.36.12";
  private static final int PORT = 20332;

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;
  private NemoEventHandler nemoEventHandler;
  private Map<Channel, EventHandler> channelEventHandlerMap;
  private AWSLambdaAsync awsLambda;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  public static final NettyServerLambdaTransport INSTANCE = new NettyServerLambdaTransport();

  private final ScheduledExecutorService warmer = Executors.newSingleThreadScheduledExecutor();

  private final int poolSize = 200;
  private final int warmupPeriod = 90; // sec

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final List<Channel> channelPool;
  private int channelCnt = 0;

  private NettyServerLambdaTransport() {
    lazyInit();
    initialized.set(true);
    this.channelPool = new ArrayList<>(poolSize);

    warmer.scheduleAtFixedRate(() -> {
      synchronized (channelPool) {
        channelPool.clear();

        for (int i = 0; i < poolSize; i++) {
          executorService.submit(() -> {
            // Trigger lambdas
            final InvokeRequest request = new InvokeRequest()
              .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
              .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
                PUBLIC_ADDRESS, PORT));
            return awsLambda.invokeAsync(request);
          });
        }

        // take
        for (int i = 0; i < poolSize; i++) {
          try {
            channelPool.add(nemoEventHandler.getHandshakeQueue().take().left());
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        // send end event
        channelPool.forEach(channel -> {
          channel.writeAndFlush(new NemoEvent(NemoEvent.Type.WARMUP_END, new byte[0], 0));
        });
      }

    }, 0, warmupPeriod, TimeUnit.SECONDS);

  }

  private void lazyInit() {
    this.serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
    this.nemoEventHandler = new NemoEventHandler(channelEventHandlerMap);
    //this.awsLambda = AWSLambdaClientBuilder.standard().withClientConfiguration(
    //  new ClientConfiguration().withMaxConnections(150)).build();
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(500)).build();
    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(this.serverBossGroup, this.serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(
        new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);
    try {
      this.acceptor = serverBootstrap.bind(
        new InetSocketAddress(ADDRESS, PORT)).sync().channel();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Object takeResult() {
    try {
      return nemoEventHandler.getResultQueue().take();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void setChannelHandler(final Channel channel, final EventHandler eventHandler) {
    channelEventHandlerMap.put(channel, eventHandler);
  }

  public Future<Channel> createLambdaChannel() {
    if (initialized.compareAndSet(false, true)) {
      lazyInit();
    }

    synchronized (channelPool) {
      final int index = (channelCnt++) % channelPool.size();
      return new Future<Channel>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return false;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }

        @Override
        public boolean isDone() {
          return false;
        }

        @Override
        public Channel get() throws InterruptedException, ExecutionException {
          return null;
        }

        @Override
        public Channel get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
          return channelPool.get(index);
        }
      };
    }

    /*
    executorService.submit(() -> {
      // Trigger lambdas
      final InvokeRequest request = new InvokeRequest()
        .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
        .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
          PUBLIC_ADDRESS, PORT));
      return awsLambda.invokeAsync(request);
    });

    return new Future<Channel>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public Channel get() throws InterruptedException, ExecutionException {
        try {
          return nemoEventHandler.getHandshakeQueue().take().left();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

      @Override
      public Channel get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
      }
    };
    */
  }


}