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
package poke.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.comm.CommConnection;
import poke.client.comm.CommListener;
import eye.Comm.Header;
import eye.Comm.JobDesc;
import eye.Comm.JobOperation;
import eye.Comm.Payload;
import eye.Comm.Ping;
import eye.Comm.Request;
import eye.Comm.JobOperation.JobAction;

/**
 * The command class is the concrete implementation of the functionality of our
 * network. One can view this as a interface or facade that has a one-to-one
 * implementation of the application to the underlining communication.
 * 
 * IN OTHER WORDS (pay attention): One method per functional behavior!
 * 
 * @author gash
 * 
 */
public class ClientCommand {
	protected static Logger logger = LoggerFactory.getLogger("client");

	private String host;
	private int port;
	private CommConnection comm;

	public ClientCommand(String host, int port) {
		this.host = host;
		this.port = port;

		init();
	}

	private void init() {
		System.out.println("In ClientCommand Init (Host:"+host+"  Port:"+port+")");
		comm = new CommConnection(host, port);
		if(comm == null)
			System.out.println("Comm is null");
	}

	/**
	 * add an application-level listener to receive messages from the server (as
	 * in replies to requests).
	 * 
	 * @param listener
	 */
	public void addListener(CommListener listener) {
		comm.addListener(listener);
	}

	/**
	 * Our network's equivalent to ping
	 * 
	 * @param tag
	 * @param num
	 */
	public void poke(String tag, int num) {
		// data to send
		Ping.Builder f = eye.Comm.Ping.newBuilder();
		f.setTag(tag);
		f.setNumber(num);

		// payload containing data
		Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setPing(f.build());
		r.setBody(p.build());

		// header with routing info
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("test finger");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.PING);
		r.setHeader(h.build());

		eye.Comm.Request req = r.build();

		try {
			//comm.channel.channel().writeAndFlush(req);
			comm.sendMessage(req);
		} catch (Exception e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}
	
	public void sendRequest(JobAction jAct, String jobId, JobDesc desc) {
		//sending a job processing request to the server
		JobOperation.Builder j = eye.Comm.JobOperation.newBuilder();
		j.setAction(jAct);
		j.setJobId(jobId);
		j.setData(desc);
		
		//payload containing data for job
		Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setJobOp(j.build());
		r.setBody(p.build());
		
		//header with routing info
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("jobs");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.JOBS);
		r.setHeader(h.build());
		
		eye.Comm.Request req = r.build();
		
		try {
			//comm.channel.channel().writeAndFlush(req);
			comm.sendMessage(req);
		} catch (Exception e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}

}
