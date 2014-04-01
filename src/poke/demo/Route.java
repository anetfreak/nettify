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
package poke.demo;

import eye.Comm.JobDesc;
import eye.Comm.JobDesc.JobCode;
import eye.Comm.JobOperation.JobAction;
import eye.Comm.NameValueSet;
import poke.client.ClientCommand;
import poke.client.ClientPrintListener;
import poke.client.comm.CommListener;

/**
 * DEMO: how to use the command class to implement a ping
 * 
 * @author gash
 * 
 */
public class Route {
	private String tag;
	private int count;

	public Route(String tag) {
		this.tag = tag;
	}

	public void run() {
		ClientCommand cc = new ClientCommand("192.168.0.27", 5570);
		CommListener listener = new ClientPrintListener("route demo");
		cc.addListener(listener);
		
		NameValueSet.Builder value = NameValueSet.newBuilder();
		value.setName("Test");
		value.setValue("Test");
		
		JobDesc.Builder desc = JobDesc.newBuilder();
		desc.setNameSpace("Engineering");
		desc.setJobId("Zero");
		desc.setOwnerId(0);
		desc.setStatus(JobCode.JOBUNKNOWN);
		desc.setOptions(value.build());
		
		cc.sendRequest(JobAction.ADDJOB, "Zero", desc.build());
	}

	public static void main(String[] args) {
		try {
			Route jab = new Route("route");
			jab.run();

			// we are running asynchronously
			System.out.println("\nExiting in 5 seconds");
			Thread.sleep(25000);
			//System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
