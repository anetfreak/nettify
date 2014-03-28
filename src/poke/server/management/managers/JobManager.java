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

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.queue.PerChannelQueue;
import poke.server.resources.ResourceFactory;
import eye.Comm.JobBid;
import eye.Comm.JobProposal;
import eye.Comm.LeaderElection;
import eye.Comm.Management;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * The job manager class is used by the system to assess and vote on a job. This
 * is used to ensure leveling of the servers take into account the diversity of
 * the network.
 * 
 * @author gash
 * 
 */
public class JobManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<JobManager> instance = new AtomicReference<JobManager>();
	private class PCQandJob
	{
		private PerChannelQueue pcq;
		private Management jp;
		public PCQandJob(PerChannelQueue pcq, Management jp)
		{
			this.pcq = pcq;
			this.jp = jp;
		}
		public PerChannelQueue getPCQ()
		{
			return pcq;
		}
		public Management getJobProposal()
		{
			return jp;
		}

	}
	//Map that will hold PerChannelQueue reference and JobProposal with JobId until response is sent to PCQ
	private Map<String,PCQandJob> queue_JobProposal = new HashMap<String,PCQandJob>();
	//All the incoming jobbid will be queued here, if i am the leader
	private Queue<JobBid> queue_JobBid = new LinkedList<JobBid>();
	//final map which will be processed and single JobBid will be sent to PerChannelQueue
	private Map<String,ArrayList<JobBid>> map_JobBid = new HashMap<String,ArrayList<JobBid>>(); 
	//TODO Timer Map, checks if time for bid expired then remove bid and send failure to client
	private Map<String,Long> map_JobIdToTime = new HashMap<String,Long>();
	private String nodeId;

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
	
	public static JobManager getInstance(String id) {
		instance.compareAndSet(null, new JobManager(id));
		return instance.get();
	}

	public static JobManager getInstance() {
		return instance.get();
	}

	public JobManager(String nodeId) {
		this.nodeId = nodeId;
		init();
	}
	protected void init()
	{
		if(isLeader())
			(new JobBidWorker(this)).start();
	}

	public boolean isLeader()
	{
		if(nodeId.equals(ElectionManager.getLeader()))
			return true;
		else
			return false;
	}
	/**
	 * Amit: Put new Job Proposal to be sent to other Servers Must be thread safe
	 * Accessed by PeChannelQueue
	 */
	public synchronized boolean submitJobProposal(PerChannelQueue sq, Management jbreq) {
		//get jobId and store
		logger.info("Chitra ka Job Proposal recieved, sending to channel");
		queue_JobProposal.put(jbreq.getJobPropose().getJobId(), new PCQandJob(sq,jbreq));
		sendResponse(jbreq);
		return true;
	}

	/**
	 * If a bid is received from a node, forward the bid request unchanged 
	 * If I am a leader, add the bidding to my Queue
	 * @param mreq
	 */
	public void sendResponse(Management mreq)
	{
		for (HeartbeatData hd : HeartbeatManager.getInstance()
				.getOutgoingQueue_test().values()) {
			logger.info("JobProposal Request beat (" + nodeId + ") sent to "
					+ hd.getNodeId() + " at " + hd.getHost()
					+ hd.channel.remoteAddress());
			try {
				if (hd.channel.isWritable()) {
					hd.channel.writeAndFlush(mreq);
				}
			} catch (Exception e) {
				logger.error("Failed  to send  for " + hd.getNodeId() + " at " + hd.getHost(), e);
			}
		}
	}

	/**
	 * a new job proposal has been sent out that I need to evaluate if I can run
	 * it
	 * 
	 * @param req
	 *            The proposal
	 */
	public void processRequest(JobProposal req) {
		if(isLeader())
		{
			//do nothing
			return;
		}
		logger.info("Received a Job Proposal");
		if (req == null) {
			logger.info("No Job Proposal request received..!");
			return;
		} else {
			logger.info("Owner of the Job Proposal : " + req.getOwnerId());
			logger.info("Job ID Received : " + req.getJobId());
			logger.info("I start to bid for the job..!");
			//startJobBidding(nodeId, req.getOwnerId(), req.getJobId());
			startJobBidding(req);
			//forward proposal request too
			Management.Builder b = Management.newBuilder();
			b.setJobPropose(req);
			sendResponse(b.build());
		}
	}

	/**
	 * a job bid for my job
	 * 
	 * @param req
	 *            The bid
	 */
	public void processRequest(JobBid req) {
		//If I am the leader, process Job Bid else forward

		if (req == null) {
			logger.info("No job bidding request received..!");
			return;
		} else {
			logger.info("Job bidding request received on channel..!");
		}

		if(isLeader())
		{
			queue_JobBid.add(req);
		}
		else
		{
			Management.Builder b = Management.newBuilder();
			b.setJobBid(req);
			sendResponse(b.build());
		}
	}

	/**
	 * Custom method for bidding for the proposed job
	 */
	public void startJobBidding(JobProposal jpReq) {
		
		for (HeartbeatData hd : HeartbeatManager.getInstance()
				.getOutgoingQueue_test().values()) {
			logger.info("Job proposal request on (" + nodeId + ") sent to "
					+ hd.getNodeId() + " at " + hd.getHost()
					+ hd.channel.remoteAddress());

			try {
				// sending job proposal request for bidding
				JobBid.Builder jb = JobBid.newBuilder();
				jb.setBid(5);
				//set own node ID as the owner for this bid
				String bidOwner = ResourceFactory.getInstance().getCfg().getServer().getProperty("node.id");
				jb.setOwnerId(NodeIdToInt(bidOwner));
				jb.setJobId(jpReq.getJobId());
				jb.setNameSpace(jpReq.getNameSpace());

				Management.Builder b = Management.newBuilder();
				b.setJobBid(jb);
				if (hd.channel.isWritable()) {
					hd.channel.writeAndFlush(b.build());
				}

			} catch (Exception e) {
				logger.error(
						"Failed  to send bidding request for " + hd.getNodeId()
						+ " at " + hd.getHost(), e);
			}

		}
	}

	/**
	 * Class for receiving all the bids from all the nodes in the cluster.
	 * If no bids are present in the job_bid queue, then Thread will wait.
	 * Once all the bids are received from the nodes, the 
	 * @author Chitra
	 *
	 */

	protected class JobBidWorker extends Thread{
		JobManager jobManager;

		public JobBidWorker(JobManager jm)
		{
			this.jobManager = jm;
		}
		public JobBid processJobBids(ArrayList<JobBid> jobBids)
		{
			JobBid finalJB = null;

			//find jobbid with highest weight
			int size = jobBids.size();
			for(int i = 0; i< size;i++)
			{
				JobBid jb = jobBids.remove(i);
				if(finalJB == null)
				{
					finalJB = jb;
				}
				else
				{
					if(finalJB.getBid() < jb.getBid())
					{
						finalJB = jb;
					}
				}
			}
			return finalJB;
		}
		@Override
		public void run() {
			while(true && isLeader())
			{
				if(jobManager.queue_JobBid.isEmpty())
				{
					//TODO can check for time here
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				else
				{
					JobBid req = jobManager.queue_JobBid.remove();
					String jobId = req.getJobId();
					if(!jobManager.map_JobBid.containsKey(jobId))
					{
						ArrayList<JobBid> jobBids = new ArrayList<JobBid>();
						jobBids.add(req);
						jobManager.map_JobBid.put(jobId,jobBids);
					}
					else
					{
						//put it in hashmap with jobid and list of jobs //TBD time
						//check if there are 3 entries //TBD or if time expires
						ArrayList<JobBid> jobBids = jobManager.map_JobBid.get(jobId);
						if(jobBids.size() >= 2)
						{
							//remove and process and send response
							JobBid finalJB = processJobBids(jobBids);
							PerChannelQueue pcq = jobManager.queue_JobProposal.get(jobId).getPCQ();
							//TOTO pcq.putBidResponse(finalJB);
							jobManager.queue_JobProposal.remove(jobId);
							jobManager.map_JobBid.remove(jobId);
							jobBids = null;
						}
						else
						{
							jobBids.add(req);
							jobManager.map_JobBid.put(jobId,jobBids);
						}
					}

				}
			}
		}
	}
}
