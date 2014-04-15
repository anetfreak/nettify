package poke.server.management.managers;

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

import java.awt.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import misc.ExtNode;
import misc.MongoDao;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.ServerHandler;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.managers.ElectionManager;
import poke.server.management.managers.HeartbeatManager;
import poke.server.resources.ResourceFactory;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Management;
import eye.Comm.Request;

public class JobConnector {
	protected static Logger logger = LoggerFactory.getLogger("JobConnector");
	MongoDao mongoDao = new MongoDao();

	private JobConnHandler handler;

	// Class to store node Info
	private class ExternalNode {
		private String nodeId;
		private String ip;
		private int port;
		private int mgmtPort;
		private Channel ch;

		public ExternalNode(String nodeId, String ip, int port, int mgmtPort) {
			this.nodeId = nodeId;
			this.ip = ip;
			this.port = port;
			this.mgmtPort = mgmtPort;
			this.ch = null;
		}

		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (getClass() != o.getClass())
				return false;
			ExternalNode e = (ExternalNode) o;
			if (this.nodeId.equals(e.nodeId))
				return true;
			return false;
		}

		public int hashCode() {
			return new HashCodeBuilder(37, 41).append(nodeId).append(ip)
					.append(port).append(mgmtPort).append(ch).toHashCode();
		}

		public String getNodeId() {
			return nodeId;
		}

		public void setNodeId(String nodeId) {
			this.nodeId = nodeId;
		}

		public String getIp() {
			return ip;
		}

		public void setIp(String ip) {
			this.ip = ip;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public int getMgmtPort() {
			return mgmtPort;
		}

		public void setMgmtPort(int mgmtPort) {
			this.mgmtPort = mgmtPort;
		}

		public Channel getCh() {
			return ch;
		}

		public void setCh(Channel ch) {
			this.ch = ch;
		}
	}

	private ArrayList<ExternalNode> ExternalNodes = new ArrayList<ExternalNode>();
	// All the outgoing connections will be stored here
	ConcurrentHashMap<String, ExternalNode> outgoingConn = new ConcurrentHashMap<String, ExternalNode>();

	// our surge protection using a in-memory cache for messages
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;

	// message processing is delegated to a threading model
	private OutboundWorker worker;

	public JobConnector() {
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();
		// start outbound message processor
		worker = new OutboundWorker(this);
		worker.start();

	}

	public void release(Channel ch) {
		
		if (ch != null)
		{
			logger.info("Releaseing a connection: ", ch.remoteAddress());
			ch.close();
		}
		// if(group != null)
		// group.shutdownGracefully();
	}

	public void sendMessage(Management req) throws Exception {
		// enqueue message
		logger.info("Received a Management request to External server");
		outbound.put(req);
	}

	private void init(ExternalNode e) {

		EventLoopGroup group = new NioEventLoopGroup();
		ExternalNode en = outgoingConn.get(e.getNodeId());
		if (en == null) {
			outgoingConn.put(e.getNodeId(), e);
			en = outgoingConn.get(e.getNodeId());
		}
		if (en.getCh() != null) {
			if ((en.getCh().isOpen()) && (en.getCh().isActive())
					&& (en.getCh().isWritable())) {
				return;
			} else {
				release(en.getCh());
				en.setCh(null);
			}
		}

		try {
			handler = new JobConnHandler(this);
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).handler(handler);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			// Make the connection attempt.
			ChannelFuture channel = b.connect(en.getIp(), en.getMgmtPort())
					.syncUninterruptibly();
			// ch.awaitUninterruptibly(5000l);
			if (channel == null)
				logger.info("Channel is null, not able to connect to Host: "
						+ en.getIp() + "  Port: " + en.getMgmtPort());
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
			pipeline.addLast("handler", new JobConnHandler(this));

			// want to monitor the connection to the server s.t. if we loose
			// the connection, we can try to re-establish it.
			ClientClosedListener ccl = new ClientClosedListener(this);
			// handler.setChannel(channel.channel());
			channel.channel().closeFuture().addListener(ccl);

			// Adding channel to hash map
			en.setCh(channel.channel());
			outgoingConn.put(en.getNodeId(), en);

		} catch (Exception ex) {
			logger.error("failed to initialize the client connection", ex);

		}
	}

	/*
	 * protected Channel connect() { // Start the connection attempt. if
	 * (channel == null) { init(); } if (channel != null) { if (channel.isDone()
	 * && channel.isSuccess() && channel.channel().isOpen() &&
	 * channel.channel().isActive() && channel.channel().isWritable()) { //
	 * System.out.println("Channel is success"); return channel.channel(); }
	 * else { release(); init(); } } return null; }
	 */
	protected void checkandChangeConn() {
		ArrayList<ExtNode> extNodes = mongoDao.findExtNodes();
		ExternalNodes.clear();
		for (ExtNode eNode : extNodes) {
			System.out.println("\nNode name : " + eNode.getName());
			System.out.println("Node IP : " + eNode.getIp());
			System.out.println("Node Port : " + eNode.getPort());
			System.out.println("Node Management Port : " + eNode.getMgmtPort()
					+ "\n");
			ExternalNode e = new ExternalNode(eNode.getName(), eNode.getIp(),
					eNode.getPort(), eNode.getMgmtPort());
			ExternalNodes.add(e);
			init(e);

			for (ExternalNode en : outgoingConn.values()) {
				if (!ExternalNodes.contains(en)) {
					release(en.getCh());
					outgoingConn.remove(en.getNodeId());
				}
			}
		}
	}

	public boolean handleIncomingRequest(Management msg) {
		if(msg.hasJobPropose())
		{
			logger.info("I will not handle job proposal at my created channel");
			//JobExternalManager.getInstance().processRequest(msg.getJobPropose());
		}
		else if(msg.hasJobBid())
			JobExternalManager.getInstance().processRequest(msg.getJobBid());
		else
			logger.info("Not a Job Proposal or Job Bid, discarding...");
		return true;
	}

	protected class OutboundWorker extends Thread {
		JobConnector conn;
		boolean forever = true;

		public OutboundWorker(JobConnector serverConnection) {
			this.conn = serverConnection;

			if (serverConnection.outbound == null)
				throw new RuntimeException(
						"connection worker detected null queue");
		}

		@Override
		public void run() {
			logger.info("ServerConnection: outbound worker started");
			// Channel ch = null;

			while (true) {
				if (!forever)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = conn.outbound.take();
					logger.info("Outbound message recieved, writing to channel");
					conn.checkandChangeConn();
					// loop through all the connected nodes and write the
					// request
					for (ExternalNode en : outgoingConn.values()) {
						// ch = conn.connect();
						Channel ch = en.getCh();
						if (ch == null || !ch.isOpen()) {
							JobConnector.logger
									.error("connection missing, no outbound communication");
							continue;
						}
						if (ch.isWritable()) {
							ch.writeAndFlush(msg);
						} else
							continue;
						// conn.outbound.putFirst(msg);
					}
				} catch (InterruptedException ie) {
					logger.error("Write Failure", ie);
					// break;
				} catch (Exception e) {
					JobConnector.logger.error(
							"Unexpected communcation failure", e);
				}
			}

			if (!forever) {
				JobConnector.logger.info("connection queue closing");
			}
		}
	}

	public static class ClientClosedListener implements ChannelFutureListener {
		JobConnector cc;

		public ClientClosedListener(JobConnector cc) {
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
