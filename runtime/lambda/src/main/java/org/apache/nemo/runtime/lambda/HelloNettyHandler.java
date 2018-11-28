package org.apache.nemo.runtime.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.NettyChannelInitializer;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class HelloNettyHandler implements RequestHandler<Map<String, Object>, Object> {

	private static final Logger LOG = LogManager.getLogger(HelloNettyHandler.class);
	//private static final OutputSender sender = new OutputSender("18.182.129.182", 20312);
	private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
    .withClientConfiguration(new ClientConfiguration().withMaxConnections(100)).build();

	private static final String BUCKET_NAME = "nemo-serverless";
	private static final String PATH = "/tmp/nexmark-0.2-SNAPSHOT-shaded.jar";
	//private static final String PATH = "/tmp/shaded.jar";
	private URLClassLoader classLoader = null;
	private LambdaSideInputHandler handler = null;

	private final String serializedUserCode = "rO0ABXNyABZRdWVyeTdTaWRlSW5wdXRIYW5kbGVyMlM6Ib0vAkQCAAB4cA==";

  /**
   * Netty event loop group for client worker.
   */
  private EventLoopGroup clientWorkerGroup;

  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> map;

	private void createClassLoader() {
		// read jar file
		final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/nexmark-0.2-SNAPSHOT-shaded.jar");
		//final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/shaded.jar");
		if (!Files.exists(Paths.get(PATH))) {
			LOG.info("Copying file...");
			final InputStream in = result.getObjectContent();
			try {
				Files.copy(in, Paths.get(PATH));
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		try {
			final URL[] urls = new URL[1];
			final File f = new File(PATH);
      urls[0] = f.toURI().toURL();
			LOG.info("File: {}, {}", f.toPath(), urls[0]);
			classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
  }

	public HelloNettyHandler() {
		LOG.info("Handler is created! {}", this);
          this.clientWorkerGroup = new NioEventLoopGroup(1,
        new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
        .channel(NioSocketChannel.class)
        .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_KEEPALIVE, true);
	}

	@Override
	public Object handleRequest(Map<String, Object> input, Context context) {
		System.out.println("Input: " + input);

		if (classLoader == null) {
			createClassLoader();
			handler = SerializeUtils.deserializeFromString(serializedUserCode, classLoader);
			LOG.info("Create class loader: {}", classLoader);
		}

		if (input.isEmpty()) {
		  // this is warmer, just return;
      System.out.println("Warm up");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }

		final List<String> result = new ArrayList<>();
    final OutputCollector outputCollector = new LambdaOutputHandler(result);

    boolean channelOpen = false;
    Channel opendChannel = null;
    for (final Map.Entry<Channel, EventHandler<NemoEvent>> entry : map.entrySet()) {
      final Channel channel = entry.getKey();
      if (!channel.isOpen()) {
        channel.close();
        map.remove(channel);
      } else {
        opendChannel = channel;
        break;
      }
    }


    if (opendChannel == null) {
      final String address = (String) input.get("address");
      final Integer port = (Integer) input.get("port");

      final ChannelFuture channelFuture;
      channelFuture = clientBootstrap.connect(new InetSocketAddress(address, port));
      channelFuture.awaitUninterruptibly();
      assert channelFuture.isDone();
      if (!channelFuture.isSuccess()) {
        final StringBuilder sb = new StringBuilder("A connection failed at Source - ");
        sb.append(channelFuture.cause());
        throw new RuntimeException(sb.toString());
      }
      opendChannel = channelFuture.channel();
      map.put(opendChannel, new LambdaEventHandler(outputCollector, opendChannel));
    }

    System.out.println("Open channel: " + opendChannel);

    // client handshake
    opendChannel.writeAndFlush(new NemoEvent(NemoEvent.Type.CLIENT_HANDSHAKE, new byte[0]));
    System.out.println("Write handshake");

    final LambdaEventHandler handler = (LambdaEventHandler) map.get(opendChannel);
    try {
      // wait until end
      System.out.println("Wait end flag");
      final Integer endFlag = handler.endBlockingQueue.take();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    //final Decoder object  = (T)ois.readObject();
		//ois.close();
		//return object;

    return result.toString();

	}

  final class LambdaEventHandler implements EventHandler<NemoEvent> {

    private WindowedValue sideInput;
    private LambdaDecoderFactory sideInputDecoderFactory;
    private LambdaDecoderFactory mainInputDecoderFactory;

    private final OutputCollector outputCollector;

    private final BlockingQueue<Integer> endBlockingQueue = new LinkedBlockingQueue<>();
    private final Channel opendChannel;

    public LambdaEventHandler(final OutputCollector outputCollector,
                              final Channel opendChannel) {
      this.outputCollector = outputCollector;
      this.opendChannel = opendChannel;
    }

    @Override
    public synchronized void onNext(final NemoEvent nemoEvent) {
      switch (nemoEvent.getType()) {
        case SIDE: {
          // receive side input
          System.out.println("Receive side");
          final ByteArrayInputStream bis = new ByteArrayInputStream(nemoEvent.getBytes());
          sideInputDecoderFactory =
            SerializeUtils.deserialize(bis, classLoader);
          try {
            final DecoderFactory.Decoder sideInputDecoder = sideInputDecoderFactory.create(bis);
            sideInput = (WindowedValue) sideInputDecoder.decode();
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        case MAIN:
          System.out.println("Receive main ");
          // receive main input
          if (sideInput == null) {
            throw new IllegalStateException("SideInput should not be null");
          }

          final ByteArrayInputStream bis = new ByteArrayInputStream(nemoEvent.getBytes());
          mainInputDecoderFactory =
            SerializeUtils.deserialize(bis, classLoader);
          try {
            final DecoderFactory.Decoder mainInputDecoder = mainInputDecoderFactory.create(bis);
            WindowedValue mainInput = null;
            int cnt = 0;
            while (true) {
              try {
                mainInput = (WindowedValue) mainInputDecoder.decode();
                handler.processMainAndSideInput(mainInput, sideInput, outputCollector);
                cnt += 1;
              } catch (final IOException e) {
                e.printStackTrace();
                if(e.getMessage().contains("EOF")) {
                  System.out.println("Cnt: " + cnt + ", eof!");
                } else {
                  System.out.println("Cnt: " + cnt + "Windowed value: " + mainInput + ", sideInput: " + sideInput + ", oc: " + outputCollector);
                  throw e;
                }
                break;
              }
            }

            sideInput = null;
            endBlockingQueue.add(1);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        case END:
          // end of event
          // update handler
          break;
      }
    }
  }

	final class LambdaOutputHandler  implements OutputCollector {

	  private final List<String> result;

	  public LambdaOutputHandler(final List<String> result) {
	    this.result = result;
    }

    @Override
    public void emit(Object output) {
      System.out.println("Emit output: " + output);
      result.add(output.toString());
    }

    @Override
    public void emitWatermark(Watermark watermark) {

    }

    @Override
    public void emit(String dstVertexId, Object output) {

    }
  }
}
