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

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.JobBid;
import eye.Comm.JobProposal;
import eye.Comm.LeaderElection;
import eye.Comm.Management;

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

	private String nodeId;

	public static JobManager getInstance(String id) {
		instance.compareAndSet(null, new JobManager(id));
		return instance.get();
	}

	public static JobManager getInstance() {
		return instance.get();
	}

	public JobManager(String nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * a new job proposal has been sent out that I need to evaluate if I can run
	 * it
	 * 
	 * @param req
	 *            The proposal
	 */
	public void processRequest(JobProposal req) {
		logger.info("Received a Job Proposal");
		if (req == null) {
			logger.info("No Job Proposal request received..!");
			return;
		}
		else
		{
			logger.info("Owner of the Job Proposal : "+req.getOwnerId());
			logger.info("Job ID Received : "+req.getJobId());
			logger.info("I start to bid for the job..!");
			startJobBidding(nodeId, req.getOwnerId(), req.getJobId());
		}
	}

	/**
	 * a job bid for my job
	 * @param req
	 *            The bid
	 */
	public void processRequest(JobBid req) {

		if (req == null) {
			logger.info("No job bidding request received..!");
			return;
		}
		else
		{
			logger.info("Job bidding request received on channel..!");
		}
	}

	/**
	 * Custom method for bidding for the proposed job
	 */
	public void startJobBidding(String lnodeId, long ownerId, String ljobId){

		for (HeartbeatData hd : HeartbeatManager.getInstance().getOutgoingQueue_test().values()) {
			logger.info("Job proposal request on (" + nodeId + ") sent to " + hd.getNodeId() + " at " + hd.getHost() + hd.channel.remoteAddress());

			try{
				//sending job proposal request for bidding 
				JobBid.Builder jb = JobBid.newBuilder();
				jb.setBid(5);
				jb.setOwnerId(ownerId);
				jb.setJobId(ljobId);

				Management.Builder b = Management.newBuilder();
				b.setJobBid(jb);
				if(hd.channel.isWritable())
				{
					hd.channel.writeAndFlush(b.build());
				}

			}catch(Exception e) {
				logger.error("Failed  to send bidding request for " + hd.getNodeId()
						+ " at " + hd.getHost(), e);
			}

		}
	}
}
