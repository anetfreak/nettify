package poke.server.management.managers;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import misc.MongoDao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.queue.PerChannelQueue;
import poke.server.resources.ResourceFactory;
import eye.Comm.JobBid;
import eye.Comm.JobProposal;
import eye.Comm.Management;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * The job manager class is used by the system to assess and vote on a job. This
 * is used to ensure leveling of the servers take into account the diversity of
 * the network.
 * 
 * @author Amit
 * 
 */
public class JobExternalManager {
	protected static Logger logger = LoggerFactory.getLogger("JobExternalManager");
	protected static AtomicReference<JobExternalManager> instance = new AtomicReference<JobExternalManager>();
	JobBidWorker jbworkerThread = null;
	JobConnector jobConn = null;
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
		if(nodeId.equals("zero") ){
				i_id = 0; 
		}
		else if(nodeId.equals("one"))
		{
				i_id = 1; 
		}
		else if(nodeId.equals("two"))
		{
				i_id = 2; 
		}
		else if (nodeId.equals("three"))
		{
				i_id =3; 
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
	
	public static JobExternalManager getInstance(String id) {
		instance.compareAndSet(null, new JobExternalManager(id));
		return instance.get();
	}

	public static JobExternalManager getInstance() {
		return instance.get();
	}

	public JobExternalManager(String nodeId) {
		logger.info("@@@@@@@@@@@Job Manager Created@@@@@@@@@@@@");
		this.nodeId = nodeId;
		init();
	}
	protected void init()
	{
		jobConn = new JobConnector();
		//if(isLeader())
			//(new JobBidWorker(this)).start();
	}
	public boolean isOwner(String o_id)
	{
		if(nodeId.equalsIgnoreCase(o_id))
			return true;
		return false;
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
		logger.info("Job Proposal recieved, sending to outside channel");
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
		logger.info("Sending Job Proposal to external server");
		try {
			jobConn.sendMessage(mreq);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Chitra : a new job proposal has been sent out that I need to evaluate if I can run it
	 * 
	 * @param req
	 *            The proposal
	 */
	public void processRequest(JobProposal req, Channel ch) {
		if(isLeader() /*&& isOwner()*/)
		{
			//TODO if(req.getOwnerId()) then do nothing
			logger.info("Received a Job Proposal from external server");
			if (req == null) {
				logger.info("No Job Proposal request received..!");
				return;
			} 
			else {
				logger.info("Owner of the Job Proposal : " + req.getOwnerId());
				logger.info("Job ID Received : " + req.getJobId());
				logger.info("I start to bid for the job..!");
				//startJobBidding(nodeId, req.getOwnerId(), req.getJobId());
				startJobBidding(req, ch);
				//forward proposal request too
				//Management.Builder b = Management.newBuilder();
				//b.setJobPropose(req);
				//sendResponse(b.build());
			}
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
			//TODO if owner then add to queue else forward
			if(jbworkerThread == null)
			{
				jbworkerThread = new JobBidWorker(this);
				jbworkerThread.start();
			}
			else if(!jbworkerThread.isAlive())
			{
				jbworkerThread.start();
			}
			//jobBids.add(req);
			//JobBid finalJB = req;
			PerChannelQueue pcq = queue_JobProposal.get(req.getJobId()).getPCQ();
			pcq.putBidResponse(req);
			queue_JobProposal.remove(req.getJobId());
			map_JobBid.remove(req.getJobId());
			//queue_JobBid.add(req);
		}
		else
		{
			//Management.Builder b = Management.newBuilder();
			//b.setJobBid(req);
			//sendResponse(b.build());
		}
	}

	/**
	 * Custom method for bidding for the proposed job
	 */
	public void startJobBidding(JobProposal jpReq, Channel ch) {
			try {
				// sending job proposal request for bidding
				JobBid.Builder jb = JobBid.newBuilder();
				
				//setting the bid randomly
				int min = 1, max = 10;
				int bid = 1;//min + (int)(Math.random() * ((max - min) + 1));
				jb.setBid(bid);
				
				//set own node ID as the owner for this bid
				String bidOwner = ResourceFactory.getInstance().getCfg().getServer().getProperty("node.id");
				
				jb.setOwnerId(NodeIdToInt(bidOwner));
				jb.setJobId(jpReq.getJobId());
				jb.setNameSpace(jpReq.getNameSpace());

				Management.Builder b = Management.newBuilder();
				b.setJobBid(jb);
				//	sendResponse(b.build());
				if(ch.isOpen() && ch.isActive() && ch.isWritable())
					ch.writeAndFlush(b.build());
			} catch (Exception e) {
				logger.error("Failed  to send bidding request to external servers");
			}

		}
	

	/**
	 * Class for receiving all the bids from all the nodes in the cluster.
	 * If no bids are present in the job_bid queue, then Thread will wait.
	 * Once all the bids are received from the nodes, the 
	 * @author Amit
	 *
	 */

	protected class JobBidWorker extends Thread{
		JobExternalManager jobManager;

		public JobBidWorker(JobExternalManager jm)
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
				JobBid jb = jobBids.remove(0);
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
			logger.info("The highest selected bid is : "+finalJB.getBid());
			return finalJB;
		}
		@Override
		public void run() {
			logger.info("Job bid worker started");
			while(true && isLeader())
			{
				//logger.info("Job Bid worker running..!!");
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
						logger.info("Adding bid to hash map, Job Id: " + jobId + " Map Size: " + jobManager.map_JobBid.size());
						ArrayList<JobBid> jobBids = jobManager.map_JobBid.get(jobId);
						
							logger.info("Selected one bid, sending it to PCQ");
							//remove and process and send response
							jobBids.add(req);
							JobBid finalJB = processJobBids(jobBids);
							PerChannelQueue pcq = jobManager.queue_JobProposal.get(jobId).getPCQ();
							pcq.putBidResponse(finalJB);
							jobManager.queue_JobProposal.remove(jobId);
							jobManager.map_JobBid.remove(jobId);
							jobBids = null;
						
					}

				}
			}
		}
	}
}

