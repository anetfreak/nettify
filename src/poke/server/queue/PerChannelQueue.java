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
package poke.server.queue;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.lang.Thread.State;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.management.managers.JobManager;
import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Header;
import eye.Comm.Header.Routing;
import eye.Comm.JobBid;
import eye.Comm.JobOperation;
import eye.Comm.JobProposal;
import eye.Comm.Management;
import eye.Comm.Payload;
import eye.Comm.PokeStatus;
import eye.Comm.Request;

/**
 * A server queue exists for each connection (channel). A per-channel queue
 * isolates clients. However, with a per-client model. The server is required to
 * use a master scheduler/coordinator to span all queues to enact a QoS policy.
 * 
 * How well does the per-channel work when we think about a case where 1000+
 * connections?
 * 
 * @author gash
 * 
 */
public class PerChannelQueue implements ChannelQueue {
	protected static Logger logger = LoggerFactory.getLogger("server");

	private Channel channel;

	// The queues feed work to the inbound and outbound threads (workers). The
	// threads perform a blocking 'get' on the queue until a new event/task is
	// enqueued. This design prevents a wasteful 'spin-lock' design for the
	// threads
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> inbound;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;

	private Queue<JobBid> bidResponse = new LinkedBlockingDeque<JobBid>();

	// This implementation uses a fixed number of threads per channel
	private OutboundWorker oworker;
	private InboundWorker iworker;

	// not the best method to ensure uniqueness
	private ThreadGroup tgroup = new ThreadGroup("ServerQueue-" + System.nanoTime());

	protected PerChannelQueue(Channel channel) {
		this.channel = channel;
		init();
	}

	public Integer NodeIdToInt(String nodeId)
	{
		Integer i_id = 0;
		switch(nodeId){
		case "zero" :
			i_id = 0; break;
		case "one" :
			i_id = 1; break;
		case "two" :
			i_id = 2; break;
		case "three" :
			i_id =3; break;
		}
		return i_id;
	}
	public String IntToNodeId(Integer i_Id)
	{
		String nodeId = "";
		switch(i_Id){
		case 0 :
			nodeId = "zero"; break;
		case 1 :
			nodeId = "one"; break;
		case 2 :
			nodeId = "two"; break;
		case 3 :
			nodeId = "three"; break;
		}
		return nodeId;
	}

	protected void init() {
		inbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		iworker = new InboundWorker(tgroup, 1, this);
		iworker.start();

		oworker = new OutboundWorker(tgroup, 1, this);
		oworker.start();

		// let the handler manage the queue's shutdown
		// register listener to receive closing of channel
		// channel.getCloseFuture().addListener(new CloseListener(this));
	}

	protected Channel getChannel() {
		return channel;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#shutdown(boolean)
	 */
	@Override
	public void shutdown(boolean hard) {
		logger.info("server is shutting down");

		channel = null;

		if (hard) {
			// drain queues, don't allow graceful completion
			inbound.clear();
			outbound.clear();
		}

		if (iworker != null) {
			iworker.forever = false;
			if (iworker.getState() == State.BLOCKED || iworker.getState() == State.WAITING)
				iworker.interrupt();
			iworker = null;
		}

		if (oworker != null) {
			oworker.forever = false;
			if (oworker.getState() == State.BLOCKED || oworker.getState() == State.WAITING)
				oworker.interrupt();
			oworker = null;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueRequest(eye.Comm.Finger)
	 */
	@Override
	public void enqueueRequest(Request req, Channel notused) {
		try {
			inbound.put(req);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueResponse(eye.Comm.Response)
	 */
	@Override
	public void enqueueResponse(Request reply, Channel notused) {
		if (reply == null)
			return;

		try {
			outbound.put(reply);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for reply", e);
		}
	}

	protected class OutboundWorker extends Thread {
		int workerId;
		PerChannelQueue sq;
		boolean forever = true;

		public OutboundWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
			super(tgrp, "outbound-" + workerId);
			this.workerId = workerId;
			this.sq = sq;

			if (outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			Channel conn = sq.channel;
			logger.info("PerChannel OutBoundWorker started...." + conn.toString() + conn.pipeline().toString());
			if (conn == null || !conn.isOpen()) {
				PerChannelQueue.logger.error("connection missing, no outbound communication");
				return;
			}

			while (true) {
				if (!forever && sq.outbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = sq.outbound.take();
					System.out.println("Got a message at server outbound queue");
					if (conn.isWritable()) {
						boolean rtn = false;
						if (channel != null && channel.isOpen() && channel.isWritable()) {
							ChannelFuture cf = channel.writeAndFlush(msg);
							cf.addListener(new WriteListener(sq));
							//ChannelFuture cf = conn.writeAndFlush(msg);
							System.out.println("Server--sending -- response");
							// blocks on write - use listener to be async
							cf.awaitUninterruptibly();
							System.out.println("Written to channel");
							rtn = cf.isSuccess();
							if (!rtn)
							{	
								System.out.println("Sending failed " + rtn + "{Reason:"+cf.cause()+"}");
								sq.outbound.putFirst(msg);
							}
						}

					} else
						sq.outbound.putFirst(msg);
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					PerChannelQueue.logger.error("Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				PerChannelQueue.logger.info("connection queue closing");
			}
		}
	}

	protected class InboundWorker extends Thread {
		int workerId;
		PerChannelQueue sq;
		boolean forever = true;

		//variable to store the jobOperation request
		Request reqOperation;


		public InboundWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
			super(tgrp, "inbound-" + workerId);
			this.workerId = workerId;
			this.sq = sq;

			if (outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			Channel conn = sq.channel;
			logger.info("PerChannel InbondWorker started");
			if (conn == null || !conn.isOpen()) {
				PerChannelQueue.logger.error("connection missing, no inbound communication");
				return;
			}

			while (true) {
				if (!forever && sq.inbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = sq.inbound.take();
					logger.info("PerChannel InbondWorker Got a message....");
					// process request and enqueue response
					if (msg instanceof Request) {
						Request req = ((Request) msg);

						// do we need to route the request?

						// handle it locally
						Resource rsc = ResourceFactory.getInstance().resourceInstance(req.getHeader());


						//String node_id = ResourceFactory.getInstance().resourceInstance(req.getHeader().)
						Request reply = null;
						if (rsc == null) 
						{
							logger.error("failed to obtain resource for " + req);
							reply = ResourceUtil.buildError(req.getHeader(), PokeStatus.NORESOURCE,
									"Request not processed");
						} 

						//if the request is for serving a job - pass the request to job manager as jobProposal
						if(req.getHeader().getRoutingId().getNumber() == Routing.JOBS.getNumber())
						{
							//reply = rsc.process(req);
							reqOperation = req;
							Management mgmt = rsc.processMgmtRequest(req);
							addJobToQueue(mgmt);
							JobBid bidReq = waitForBid();
							createJobOperation(bidReq);
						}
						else
						{
							reply = rsc.process(req);
							sq.enqueueResponse(reply, null);
						}
					}

				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					PerChannelQueue.logger.error("Unexpected processing failure", e);
					break;
				}
			}

			if (!forever) {
				PerChannelQueue.logger.info("connection queue closing");
			}
		}

		//method to place a request of job processing on Job Manager
		public void addJobToQueue(Management jobReq){
			JobManager.getInstance().submitJobProposal(sq, jobReq);
		}



		public JobBid waitForBid(){
			while(true)
			{
				if(bidResponse.isEmpty())
				{
					try {
						this.sleep(200);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				return bidResponse.remove();
			}
		}

		public Request createJobOperation(JobBid bidReq){

			int recNode = (int) (bidReq.getOwnerId());
			String sendReqtoNode = IntToNodeId(recNode);
			String myNodeId = ResourceFactory.getInstance().getCfg().getServer().getProperty("node.id");
			logger.info("Adding the bid to my own queue and forwarding the job proposal to the respective node");
			logger.info("Bid received for serving the request from the node : "+sendReqtoNode);

			//modifying the joboperation header and payload - sending the message to the node which has won the bid
			Request jobOpRequest = reqOperation;

			JobOperation.Builder j = JobOperation.newBuilder();
			j.setAction(jobOpRequest.getBody().getJobOp().getAction());
			j.setJobId(jobOpRequest.getBody().getJobOp().getJobId());
			j.setData(jobOpRequest.getBody().getJobOp().getData());

			//payload containing data for job
			Request.Builder r = Request.newBuilder();
			eye.Comm.Payload.Builder p = Payload.newBuilder();
			p.setJobOp(j.build());
			r.setBody(p.build());

			//header with routing info
			Header.Builder h = Header.newBuilder();
			h.setOriginator(myNodeId);
			h.setTag(jobOpRequest.getHeader().getTag());
			h.setTime(jobOpRequest.getHeader().getTime());
			h.setRoutingId(jobOpRequest.getHeader().getRoutingId());
			h.setToNode(sendReqtoNode);

			r.setHeader(h.build());
			Request req = r.build();

			return req;
		}

	}

	/**
	 * If a response is received after bidding is done, forward the job operation request
	 * to the server which has sent the bid
	 * @param bidReq
	 */
	public void putBidResponse(JobBid bidReq){
		bidResponse.add(bidReq);
	}


	public class WriteListener implements ChannelFutureListener {
		private ChannelQueue sq;

		public WriteListener(ChannelQueue sq) {
			this.sq = sq;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			logger.info("Write complete");
			logger.info("isSuccess: " +future.isSuccess());
			logger.info("Cause: " + future.cause());
			//sq.shutdown(true);
		}
	}

	public class CloseListener implements ChannelFutureListener {
		private ChannelQueue sq;

		public CloseListener(ChannelQueue sq) {
			this.sq = sq;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			sq.shutdown(true);
		}
	}
}
