package poke.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.ServerHandler;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.managers.ElectionManager;
import poke.server.management.managers.HeartbeatManager;
import poke.server.resources.ResourceFactory;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Request;

public class ServerConnection extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("ServerConnection");
	private String host;
	private int port;
	public ChannelFuture channel; // do not use directly call connect()!
	private EventLoopGroup group;
	private ServerConnHandler handler;

	// our surge protection using a in-memory cache for messages
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;

	// message processing is delegated to a threading model
	private OutboundWorker worker;

	public ServerConnection() {
		host = null;
		port = 0;
	}
	
	

	public void release() {
		group.shutdownGracefully();
	}

	public void sendMessage(Request req) throws Exception {
		// enqueue message
		outbound.put(req);
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();
	}

	private void init() {
		// the queue to support client-side surging
		group = new NioEventLoopGroup();
		try {
			handler = new ServerConnHandler();
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).handler(handler);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			// Make the connection attempt.
			channel = b.connect(host, port).syncUninterruptibly();
			channel.awaitUninterruptibly(5000l);
			if (channel == null)
				logger.info("Channel is null, not able to connect to Host: "+ host + "  Port: " + port);
			else
				System.out.println("Channel created, not null");

			// setting pipeline parameters
			ChannelPipeline pipeline = channel.channel().pipeline();
			pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
					67108864, 0, 4, 0, 4));
			pipeline.addLast("protobufDecoder", new ProtobufDecoder(
					eye.Comm.Request.getDefaultInstance()));
			pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
			pipeline.addLast("protobufEncoder", new ProtobufEncoder());
			pipeline.addLast("handler", handler);

			// want to monitor the connection to the server s.t. if we loose the
			// connection, we can try to re-establish it.
			ClientClosedListener ccl = new ClientClosedListener(this);
			//handler.setChannel(channel.channel());
			channel.channel().closeFuture().addListener(ccl);

		} catch (Exception ex) {
			logger.error("failed to initialize the client connection", ex);

		}

		// start outbound message processor
		worker = new OutboundWorker(this);
		worker.start();
	}

	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
			init();
		}

		if (channel.isDone() && channel.isSuccess()) {
			System.out.println("Channel is success");
			return channel.channel();
		} else
			throw new RuntimeException(
					"Not able to establish connection to server");
	}

	protected void checkandChangeConn()
	{
		String currNode = ElectionManager.getInstance().getCurrentNode();
		ServerConf conf = ResourceFactory.getInstance().getCfg();
		NodeDesc node = conf.getNearest().getNearestNodes().get(currNode);
		String nextHost = node.getHost();
		int nextPort = node.getPort();
		if(!nextHost.equals(host) || nextPort != port )
		{
			logger.info("Next Node Changed from host: " + host + ":"+port + " to host: " + nextHost + ":" + nextPort);
			//Reset Connection
			this.host = nextHost;
			this.port = nextPort;
			release();
			init();
		}
	}
	
	protected class OutboundWorker extends Thread {
		ServerConnection conn;
		boolean forever = true;

		public OutboundWorker(ServerConnection serverConnection) {
			this.conn = serverConnection;

			if (serverConnection.outbound == null)
				throw new RuntimeException(
						"connection worker detected null queue");
		}

		@Override
		public void run() {
			Channel ch = null;
			
			while (true) {
				if (!forever && conn.outbound.size() == 0)
					break;
				//Check if connection to next node changed, if yes then connect to new neighbour
				checkandChangeConn();
				ch = conn.connect();
				if (ch == null || !ch.isOpen()) {
					ServerConnection.logger.error("connection missing, no outbound communication");
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
				
				try {
					// block until a message is enqueued
					GeneratedMessage msg = conn.outbound.take();
					// System.out.println("Message received");
					if (ch.isWritable()) {
						ch.writeAndFlush(msg);
					} else
						conn.outbound.putFirst(msg);
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					ServerConnection.logger.error(
							"Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				ServerConnection.logger.info("connection queue closing");
			}
		}
	}

	public static class ClientClosedListener implements ChannelFutureListener {
		ServerConnection cc;

		public ClientClosedListener(ServerConnection cc) {
			this.cc = cc;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			// we lost the connection or have shutdown.
			System.out.println("Channel closed");
			// @TODO if lost, try to re-establish the connection
		}
	}
}
