package org.apache.nemo.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.io.Serializable;
import java.util.List;

public final class NemoEventCoder {

  public static final class NemoEventEncoder extends MessageToMessageEncoder<NemoEvent> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NemoEvent msg, List<Object> out) throws Exception {
      if (msg.getByteBuf() != null) {
        out.add(msg.getByteBuf());
      } else {
        final ByteBuf buf = ctx.alloc().buffer(4 + msg.getLen());
        //System.out.println("Encoded bytes: " + msg.getLen() + 8);
        buf.writeInt(msg.getType().ordinal());
        buf.writeBytes(msg.getBytes(), 0, msg.getLen());
        out.add(buf);
      }
    }
  }

  public static final class NemoEventDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {

      //System.out.println("Decoded bytes: " + msg.readableBytes());
      final int typeOrdinal = msg.readInt();

      // copy the ByteBuf content to a byte array
      //byte[] array = new byte[msg.readableBytes()];
      //msg.readBytes(array);

      try {
        out.add(new NemoEvent(NemoEvent.Type.values()[typeOrdinal], msg.retain(1)));
      } catch (final ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        System.out.println("Type ordinal: " + typeOrdinal);
        throw e;
      }
    }
  }

}
