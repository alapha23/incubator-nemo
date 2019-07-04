package org.apache.nemo.runtime.executor.relayserver;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

public final class RelayServerChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger LOG = LoggerFactory.getLogger(RelayServerChannelInitializer.class);

  private final ConcurrentMap<String, Channel> taskChannelMap;
  private final ConcurrentMap<String, List<ByteBuf>> pendingBytes = new ConcurrentHashMap<>();

  public RelayServerChannelInitializer(
    final ConcurrentMap<String, Channel> taskChannelMap) {
    this.taskChannelMap = taskChannelMap;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    LOG.info("Registering channel {}", ch.remoteAddress());
    // DO nothing!!

    ch.pipeline().addLast(new RelayServerDecoder(taskChannelMap, pendingBytes));
  }
}
