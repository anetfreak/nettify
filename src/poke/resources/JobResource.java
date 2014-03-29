/*
 * copyright 2012, gash
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
package poke.resources;

import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;
import eye.Comm.JobProposal;
import eye.Comm.Management;
import eye.Comm.NameValueSet;
import eye.Comm.NameValueSet.NodeType;
import eye.Comm.Payload;
import eye.Comm.Ping;
import eye.Comm.PokeStatus;
import eye.Comm.Request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobResource implements Resource {

	protected static Logger logger = LoggerFactory.getLogger("server");
	
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
	
	@Override
	public Request process(Request request) {
		// TODO Auto-generated method stub
		//the request received is a job serving request
		
//		logger.info("Creating a job processing request ..! "+request.getBody().getJobOp().getJobId());
//
//		String nodeId = ResourceFactory.getInstance().getCfg().getServer().getProperty("node.id");
//		Request.Builder rb = Request.newBuilder();
//		// metadata
//		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS, null));
//
//		// payload
//		Management.Builder b = Management.newBuilder();
//		JobProposal.Builder jp = JobProposal.newBuilder();
//		jp.setJobId(request.getBody().getJobOp().getJobId());
//		jp.setOwnerId(Long.parseLong(nodeId));
//		jp.setWeight(4);
//		
//		b.setJobPropose(jp);
//		Management reply = b.build();
		return request;
	}
	
	public Management processMgmtRequest(Request request) {
		// TODO Auto-generated method stub
		//the request received is a job serving request
		
		logger.info("Creating a job processing request ..! "+request.getBody().getJobOp().getData().getJobId());

		String nodeId = ResourceFactory.getInstance().getCfg().getServer().getProperty("node.id");
		Request.Builder rb = Request.newBuilder();
		// metadata
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS, null));

		// payload
		Management.Builder b = Management.newBuilder();
		JobProposal.Builder jp = JobProposal.newBuilder();
		
		jp.setJobId(request.getBody().getJobOp().getData().getJobId());
		jp.setOwnerId(NodeIdToInt(nodeId));
		
		//setting the weight of the proposal - how heavy is the request
		int min = 1, max = 10;
		int weight = min + (int)(Math.random() * ((max - min) + 1));
		jp.setWeight(weight);
		
		NameValueSet.Builder nameVal = NameValueSet.newBuilder();
		nameVal.setNodeType(NodeType.NODE);
		nameVal.setName(nodeId);
		jp.setOptions(nameVal);
		
		if(request.getBody().getJobOp().getData().hasNameSpace())
			jp.setNameSpace(request.getBody().getJobOp().getData().getNameSpace());
		else
			jp.setNameSpace("JobProposal");
		
		b.setJobPropose(jp);
		Management reply = b.build();
		return reply;
	}

}
