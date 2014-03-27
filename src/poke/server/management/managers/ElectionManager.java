/*
 * copyright 2014, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.management.managers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;

import poke.monitor.HeartMonitor.MonitorClosedListener;
import poke.server.management.ManagementInitializer;
import poke.server.management.ManagementQueue;
import poke.server.management.ManagementQueue.ManagementQueueEntry;
import poke.server.management.managers.HeartbeatManager.CloseHeartListener;
import eye.Comm.LeaderElection;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Management;

/**
 * The election manager is used to determine leadership within the network.
 * 
 * @author gash
 * 
 */
public class ElectionManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();
	private ChannelFuture cf;
	private String nodeId;
	private String LeaderId;

	class ElectionNearestNode {
		Channel ch;
		SocketAddress sa;
		String nodeId;
		String host;
		Integer port;
		Integer mgmtPort;

		public ElectionNearestNode(Channel ch, SocketAddress sa, String nodeId,
				String host, Integer port, Integer mgmtPort) {
			this.ch = ch;
			this.sa = sa;
			this.nodeId = nodeId;
			this.host = host;
			this.port = port;
			this.mgmtPort = mgmtPort;
		}

		public void setChannel(Channel ch) {
			this.ch = ch;
		}

		public void setSocket(SocketAddress sa) {
			this.sa = sa;
		}

		public Channel getChannel() {
			return this.ch;
		}

		public SocketAddress getSA() {
			return this.sa;
		}

		public String getNodeId() {
			return this.nodeId;
		}

		public String getHost() {
			return this.host;
		}

		public Integer getPort() {
			return this.port;
		}

		public Integer getMgmtPort() {
			return this.mgmtPort;
		}
	}

	ArrayList<ElectionNearestNode> list_nearestNode = new ArrayList<ElectionNearestNode>();
	/** @brief the number of votes this server can cast */
	private int votes = 1;
	private VoteAction status = VoteAction.ELECTION;

	public void setStatus(VoteAction status) {
		this.status = status;
	}

	public VoteAction getStatus() {
		return this.status;
	}

	public static ElectionManager getInstance(String id, int votes) {
		instance.compareAndSet(null, new ElectionManager(id, votes));
		return instance.get();
	}

	public static ElectionManager getInstance() {
		return instance.get();
	}

	/**
	 * initialize the manager for this server
	 * 
	 * @param nodeId
	 *            The server's (this) ID
	 */
	protected ElectionManager(String nodeId, int votes) {
		this.nodeId = nodeId;

		if (votes >= 0)
			this.votes = votes;
	}

	public void addOutgoingChannel() {
/*
		// logger.info("adding channel in EL");
		// ElectionNearestNode enn = new ElectionNearestNode(ch, sa);
		// list_nearestNode.add(enn);
		//logger.info("in addOutgoing Channel , NodeId: "	);
		for (int i = 0; i < list_nearestNode.size(); i++) {
			// if (list_nearestNode.get(i).getNodeId().equals(nodeId)) {
			if (list_nearestNode.get(i).getChannel() == null) {
				Bootstrap b = new Bootstrap();
				EventLoopGroup group = new NioEventLoopGroup();
				ChannelFuture channel;
				// @TODO newFixedThreadPool(2);
				boolean compressComm = false;
				b.group(group).channel(NioSocketChannel.class)
						.handler(new ManagementInitializer(compressComm));
				b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// boolean compressComm = false;
				// b.(new ManagementInitializer(compressComm));

				// Make the connection attempt.
				// logger.info("channel connecting to host " + host + " port: "
				// + mgmtport);
				channel = b.connect(list_nearestNode.get(i).getHost(),
						list_nearestNode.get(i).getMgmtPort())
						.syncUninterruptibly();
				channel.awaitUninterruptibly(5000l);
				channel.channel()
						.closeFuture()
						.addListener(
								new CloseChannelListener(list_nearestNode
										.get(i).getNodeId()));
				cf = channel;
				if (cf != null)
					if (channel.channel().isOpen())
					{	if(channel.channel().isWritable())
						list_nearestNode.get(i).setChannel(cf.channel());
						logger.info("added to outgoing queue of Election manager, NodedId: "
								+ nodeId);
						
						logger.info("Channel is created and is open");
					}
					else {
						logger.info("Channel is not open");
						return;
					}
				// ElectionNearestNode enn = new ElectionNearestNode(ch, sa);
				// list_nearestNode.add(enn);
				// ch.closeFuture().addListener(new CloseChannelListener(enn));
				
				// }

			}
		}
		*/
	}

	/**
	 * @param args
	 */
	public void processRequest(LeaderElection req) {
		logger.info("Got Election request to process");
		if (req == null) {
			logger.info("req is NULL, do something");
			return;
		}

		if (req.hasExpires()) {
			long ct = System.currentTimeMillis();
			if (ct > req.getExpires()) {
				logger.info("Election is over");
				// election is over
				return;
			}
		}

		if (req.getVote().getNumber() == VoteAction.ELECTION_VALUE) {
			// an election is declared!
			setStatus(VoteAction.ELECTION);
			logger.info("an election is declared by node " + req.getNodeId());
		} 
		else if (req.getVote().getNumber() == VoteAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode`
			logger.info("no one was elected, I am dropping into standby mode");
		} 
		else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) {
			// some node declared them self the leader
			LeaderId = req.getBallotId();
			if(!req.getNodeId().contentEquals(nodeId))
				createAndSend_test(req.getNodeId(), VoteAction.DECLAREWINNER, req.getBallotId(),"I am the winner");
			setStatus(VoteAction.DECLAREWINNER);
			logger.info("node " + req.getBallotId()	+ " declared them self the leader");
		} 
		else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// for some reason, I decline to vote
			setStatus(VoteAction.ABSTAIN);
			logger.info("for some reason, I decline to vote");
		} 
		else if (req.getVote().getNumber() == VoteAction.NOMINATE_VALUE) {
			if(getStatus() == VoteAction.DECLAREWINNER)
			{
				createAndSend_test(nodeId, VoteAction.DECLAREWINNER, LeaderId,"I am the winner");
				return;
			}
			setStatus(VoteAction.NOMINATE);
			logger.info("in Nominate Value, checking the node id : "+ req.getNodeId());
			int comparedToMe = req.getNodeId().compareTo(nodeId);
			if (comparedToMe <= -1) {
				// Someone else has a higher priority, forward nomination
				logger.info("Someone else has a higher priority, forward nomination");
				createAndSend_test(req.getNodeId(), req.getVote(),req.getBallotId(), req.getDesc());
				setStatus(VoteAction.NOMINATE);
				// TODO forward
			} 
			else if (comparedToMe >= 1) {
				// I have a higher priority, nominate myself
				logger.info("I have a higher priority, nominate myself");
				createAndSend_test(nodeId, VoteAction.NOMINATE, nodeId,"Nominating myself");
				setStatus(VoteAction.NOMINATE);
				// TODO nominate myself
			} 
			else {
				logger.info("I am the node, Declaring myself as the winner!! Hurrrrey!! :) OCG");
				createAndSend_test(req.getNodeId(), VoteAction.DECLAREWINNER, req.getBallotId(),"I am the winner");
				LeaderId = req.getBallotId();
				setStatus(VoteAction.DECLAREWINNER);
			}
		}
	}

	public void createAndSend_test(String lnodeId, VoteAction vote,
			String ballotId, String desc)
	{
		
		for (HeartbeatData hd : HeartbeatManager.getInstance().getOutgoingQueue_test().values()) {
			logger.info("EL beat (" + nodeId + ") sent to " + hd.getNodeId() + " at " + hd.getHost() + hd.channel.remoteAddress());
			
			try{
			LeaderElection.Builder h = LeaderElection.newBuilder();
			h.setNodeId(lnodeId);
			h.setVote(vote);
			h.setBallotId(ballotId);
			h.setDesc(desc);

			Management.Builder b = Management.newBuilder();
			b.setElection(h.build());
			if(hd.channel.isWritable())
			{
				hd.channel.writeAndFlush(b.build());
			}
		}catch(Exception e) {
			//hd.incrementFailuresOnSend();
			logger.error("Failed  to send  for " + hd.getNodeId()
					+ " at " + hd.getHost(), e);
		}
		
		}
		
	}
	
	public boolean createAndSend(String lnodeId, VoteAction vote,
			String ballotId, String desc) {
		for (int i = 0; i < list_nearestNode.size(); i++) {
			logger.info("There is nearest node, send EL");
			if (list_nearestNode.get(i).getChannel() != null) {

				LeaderElection.Builder h = LeaderElection.newBuilder();
				h.setNodeId(lnodeId);
				h.setVote(vote);
				h.setBallotId(ballotId);
				h.setDesc(desc);

				Management.Builder b = Management.newBuilder();
				b.setElection(h.build());
				list_nearestNode.get(i).getChannel().writeAndFlush(b.build());
			}
		}
		return true;

	}

	// Nominate for self and start the election
	public void startElectionByVote() {
		logger.info("in startElectionByVote");
		for (int i = 0; i < list_nearestNode.size(); i++) {
			logger.info("There is nearest node, send EL");
			if (list_nearestNode.get(i).getChannel() != null) {
				LeaderElection.Builder h = LeaderElection.newBuilder();
				h.setNodeId(nodeId);
				h.setVote(VoteAction.NOMINATE);
				h.setBallotId(nodeId);
				h.setDesc("Nominating myself");

				Management.Builder b = Management.newBuilder();
				b.setElection(h.build());

				if (logger.isDebugEnabled())
					logger.debug("Inbound management message received");
				logger.info("startElectionByVote, sending election msg");
				// ManagementQueue.enqueueResponse(b.build(), );
				// ManagementQueue.enqueueRequest(b.build(),
				// list_nearestNode.get(i)
				// .getChannel(), list_nearestNode.get(i).getSA());

				// ManagementQueue.enqueueResponse(b.build(),
				// list_nearestNode.get(i).getChannel());
				// if (cf != null) {
				// logger.info("cf is not null, enqueue msg");
				list_nearestNode.get(i).getChannel().writeAndFlush(b.build());
				// ManagementQueue.enqueueResponse(b.build(),
				// list_nearestNode.get(i).getChannel());
			}

		}
	}

	public void setChannel(String nodeId, String host, Integer port,
			Integer mgmtPort) {
		ElectionNearestNode enn = new ElectionNearestNode(null, null, nodeId,
				host, port, mgmtPort);
		logger.info("Adding to nearest node List:");
		logger.info("NodeId: " + nodeId + "host: " + host + "port: " + port
				+ "MgmtPort: " + mgmtPort);
		list_nearestNode.add(enn);
		// cf = f;
	}

	public class CloseChannelListener implements ChannelFutureListener {
		private String nodeId;

		public CloseChannelListener(String nodeId) {
			this.nodeId = nodeId;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			for (int i = 0; i < list_nearestNode.size(); i++) {
				logger.info("Channle closing with node id: " + nodeId + "  " + list_nearestNode.get(i).getNodeId());
				if (list_nearestNode.get(i).getNodeId().equals(nodeId)) {
					logger.info("OK fine");
					list_nearestNode.get(i).setChannel(null);
					list_nearestNode.get(i).setSocket(null);
				}
			}
			// if (list_nearestNode.contains(enn)) {
			// logger.warn("EL_Mgr outgoing channel closing for node");// '" +
			// enn.getChannel()
			// +
			// "' at "
			// +
			// heart.getHost());
			// list_nearestNode.remove(enn);

			// else if (incomingHB.containsValue(heart)) {
			// logger.warn("HB incoming channel closing for node '" +
			// heart.getNodeId() + "' at " + heart.getHost());
			// incomingHB.remove(future.channel());
			// }
		}
	}
}
