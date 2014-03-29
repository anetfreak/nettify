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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.LeaderElection.VoteAction;

import poke.monitor.HeartMonitor;
import poke.server.management.managers.HeartbeatData.BeatStatus;
import poke.server.resources.ResourceFactory;

/**
 * The connector collects connection monitors (e.g., listeners implement the
 * circuit breaker) that maintain HB communication between nodes (to
 * client/requester).
 * 
 * @author gash
 * 
 */

public class HeartbeatConnector extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<HeartbeatConnector> instance = new AtomicReference<HeartbeatConnector>();

	private ConcurrentLinkedQueue<HeartMonitor> monitors = new ConcurrentLinkedQueue<HeartMonitor>();
	private int sConnectRate = 2000; // msec
	private boolean forever = true;
	private String nodeId;
	public static HeartbeatConnector getInstance() {
		instance.compareAndSet(null, new HeartbeatConnector());
		return instance.get();
	}
	//Amit trying to implement automatic handling on ring
	public void setNodeId(String nodeId)
	{
		this.nodeId = nodeId;
	}
	/**
	 * The connector will only add nodes for connections that this node wants to
	 * establish. Outbound (we send HB messages to) requests do not come through
	 * this class.
	 * 
	 * @param node
	 */
	public void addConnectToThisNode(HeartbeatData node) {
		// null data is not allowed
		if (node == null || node.getNodeId() == null)
			throw new RuntimeException("Null nodes or IDs are not allowed");

		// register the node to the manager that is used to determine if a
		// connection is usable by the public messaging
		HeartbeatManager.getInstance().addAdjacentNode(node);

		// this class will monitor this channel/connection and together with the
		// manager, we create the circuit breaker pattern to separate
		// health-status from usage.
		HeartMonitor hm = new HeartMonitor(node.getNodeId(), node.getHost(), node.getMgmtport());
		hm.addListener(new HeartbeatListener(node));

		// add monitor to the list of adjacent nodes that we track
		monitors.add(hm);
	}
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
			
	private void checkIfLeaderDown(int Leader, int nextNode, int myNode)
	{
		logger.info("Leader: "+ Leader + " NextNode:" + nextNode + " MyNode: " + myNode);
		if(Leader > myNode)
		{
			if(nextNode > Leader || nextNode < myNode)
				ElectionManager.getInstance().setStatus(VoteAction.ELECTION);
		}
		else if(Leader < myNode)
		{
			if(nextNode > Leader && nextNode < myNode)
				ElectionManager.getInstance().setStatus(VoteAction.ELECTION);
		}
		/*
		if(currentNode < NodeIdToInt(nodeId))
		{
			if(nextNode < currentNode)
			{
				//leader is down redo election
				ElectionManager.getInstance().setStatus(VoteAction.ELECTION);
			}
		}
		else
		{
			if(nextNode > currentNode)
			{
				//leader is down, redo election
				ElectionManager.getInstance().setStatus(VoteAction.ELECTION);
			}
		}
		*/

	}
	
	@Override
	public void run() {
		if (monitors.size() == 0) {
			logger.info("HB connection monitor not started, no connections to establish");
			return;
		} else
			logger.info("HB connection monitor starting, node has " + monitors.size() + " connections");
		Integer total_nodes = Integer.parseInt(ResourceFactory.getCfg().getServer().getProperty("total_nodes"));
		//Amit trying to implement automatic handling on ring
		Map<Integer,HeartMonitor> map_monitors = new HashMap<Integer,HeartMonitor>();
		Integer currentNode = (NodeIdToInt(this.nodeId)+1)%total_nodes;
		for (HeartMonitor hb : monitors) {
			System.out.println("Adding monitor to map id: "+NodeIdToInt(hb.getNodeId()));
			map_monitors.put(NodeIdToInt(hb.getNodeId()), hb);
		}
		
		while (forever) {
			try {
				Thread.sleep(sConnectRate);

				// try to establish connections to our nearest nodes
				//Amit trying to implement automatic handling on ring
				//for (HeartMonitor hb : monitors) {
				for(int i = 0;i<total_nodes;i++){
					Integer nextNode = (NodeIdToInt(this.nodeId) + 1 + i)%total_nodes; //next node to which this node should connect in Ring
					if(nextNode == NodeIdToInt(this.nodeId)) //if next node is this node then continue
						continue;
					HeartMonitor hb = map_monitors.get(nextNode);
					if (!hb.isConnected()) {
						try {
							for(int j =0;j<2;j++)
							{
								logger.info("attempting to connect to node: " + hb.getNodeInfo());
								hb.startHeartbeat();
								Thread.sleep(500);
								if(hb.isConnected())
								{
									logger.info("Connected to node :"+hb.getNodeInfo());
									
									if(currentNode != nextNode && currentNode != NodeIdToInt(nodeId))
									{	
										//checking is leader is down and redo election if so
										if(currentNode == NodeIdToInt(ElectionManager.getLeader()))
										{
											checkIfLeaderDown(currentNode, nextNode, NodeIdToInt(nodeId));
										}
										logger.info("1. Switching from "+ currentNode + " ==> "+nextNode);
										logger.info("Disconnecting from Node: " + map_monitors.get(currentNode).getNodeInfo());
										if(map_monitors.get(currentNode).isConnected())
											map_monitors.get(currentNode).closeConn(); 
										currentNode = nextNode;
										ElectionManager.setCurrentNode(IntToNodeId(currentNode));
										logger.info("Now Current Node is " + currentNode);
									}
									if(!IntToNodeId(currentNode).equals(ElectionManager.getCurrentNode()))
										ElectionManager.setCurrentNode(IntToNodeId(currentNode));
									break;
								}
									
							}
							if(hb.isConnected())
							{
								if(currentNode != nextNode && currentNode != NodeIdToInt(nodeId))
								{
									logger.info("Connected to node outside loop :"+hb.getNodeInfo());
									logger.info("2. Switching from "+currentNode + " ==> "+nextNode);
									if(map_monitors.get(currentNode).isConnected())
										map_monitors.get(currentNode).closeConn();
									currentNode = nextNode;
									ElectionManager.setCurrentNode(IntToNodeId(currentNode));
								}
								if(!IntToNodeId(currentNode).equals(ElectionManager.getCurrentNode()))
									ElectionManager.setCurrentNode(IntToNodeId(currentNode));
								break;
							}
							
						} catch (Exception ie) {
							logger.error("Exception: HeartBeatConnecton",ie);
						}
					}
					else
					{
						if(currentNode != nextNode && currentNode != NodeIdToInt(nodeId))
						{
							logger.info("3. Switching from "+currentNode + " ==> "+nextNode);
							if(map_monitors.get(currentNode).isConnected())
								map_monitors.get(currentNode).closeConn();
							currentNode = nextNode;
							ElectionManager.setCurrentNode(IntToNodeId(currentNode));
						}
						if(!IntToNodeId(currentNode).equals(ElectionManager.getCurrentNode()))
								ElectionManager.setCurrentNode(IntToNodeId(currentNode));
						break;
					}
				}
				
				for(int k = 0;k<total_nodes;k++){
					if(k != NodeIdToInt(nodeId) && k != currentNode)
					{
						HeartMonitor hb = map_monitors.get(k);
						if(hb.isConnected())
						{
							logger.info("Disconnecting from Node: " + hb.getNodeInfo());
							hb.closeConn();
						}
					}
				}
				
			} catch (InterruptedException e) {
				logger.error("Unexpected HB connector failure", e);
				break;
			}
		}
		logger.info("ending heartbeatMgr connection monitoring thread");
	}

	private void validateConnection() {
		// validate connections this node wants to create
		for (HeartbeatData hb : HeartbeatManager.getInstance().incomingHB.values()) {
			// receive HB - need to check if the channel is readable
			if (hb.channel == null) {
				if (hb.getStatus() == BeatStatus.Active || hb.getStatus() == BeatStatus.Weak) {
					hb.setStatus(BeatStatus.Failed);
					hb.setLastFailed(System.currentTimeMillis());
					hb.incrementFailures();
				}
			} else if (hb.channel.isOpen()) {
				if (hb.channel.isWritable()) {
					if (System.currentTimeMillis() - hb.getLastBeat() >= hb.getBeatInterval()) {
						hb.incrementFailures();
						hb.setStatus(BeatStatus.Weak);
					} else {
						hb.setStatus(BeatStatus.Active);
						hb.setFailures(0);
					}
				} else
					hb.setStatus(BeatStatus.Weak);
			} else {
				if (hb.getStatus() != BeatStatus.Init) {
					hb.setStatus(BeatStatus.Failed);
					hb.setLastFailed(System.currentTimeMillis());
					hb.incrementFailures();
				}
			}
		}

		// validate connections this node wants to create
		for (HeartbeatData hb : HeartbeatManager.getInstance().outgoingHB.values()) {
			// emit HB - need to check if the channel is writable
			if (hb.channel == null) {
				if (hb.getStatus() == BeatStatus.Active || hb.getStatus() == BeatStatus.Weak) {
					hb.setStatus(BeatStatus.Failed);
					hb.setLastFailed(System.currentTimeMillis());
					hb.incrementFailures();
				}
			} else if (hb.channel.isOpen()) {
				if (hb.channel.isWritable()) {
					if (System.currentTimeMillis() - hb.getLastBeat() >= hb.getBeatInterval()) {
						hb.incrementFailures();
						hb.setStatus(BeatStatus.Weak);
					} else {
						hb.setStatus(BeatStatus.Active);
						hb.setFailures(0);
					}
				} else
					hb.setStatus(BeatStatus.Weak);
			} else {
				if (hb.getStatus() != BeatStatus.Init) {
					hb.setStatus(BeatStatus.Failed);
					hb.setLastFailed(System.currentTimeMillis());
					hb.incrementFailures();
				}
			}
		}
	}
}
