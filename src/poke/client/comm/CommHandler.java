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
package poke.client.comm;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.protobuf.GeneratedMessage;


@CommHandler.Sharable
public class CommHandler extends SimpleChannelInboundHandler<eye.Comm.Request> {
	protected static Logger logger = LoggerFactory.getLogger("connect");
	protected ConcurrentMap<String, CommListener> listeners = new ConcurrentHashMap<String, CommListener>();
	private volatile Channel channel;

	public CommHandler() {
	}
	public void setChannel(Channel channel)
	{
		this.channel =channel;
	}
	
	
	/**
	 * messages pass through this method. We use a blackbox design as much as
	 * possible to ensure we can replace the underlining communication without
	 * affecting behavior.
	 * 
	 * @param msg
	 * @return
	 */
	public boolean send(GeneratedMessage msg) {
		// TODO a queue is needed to prevent overloading of the socket
		// connection. For the demonstration, we don't need it
		if(channel != null)
		{
			
			ChannelFuture cf = channel.writeAndFlush(msg);
			cf.awaitUninterruptibly();
			System.out.println("In send ** Channel Handler** success");
			if (cf.isDone() && !cf.isSuccess()) {
				logger.error("failed to poke!");
				return false;
			}
		}
		else
			System.out.println("Channel is null");

		return true;
	}

	/**
	 * Notification registration. Classes/Applications receiving information
	 * will register their interest in receiving content.
	 * 
	 * Note: Notification is serial, FIFO like. If multiple listeners are
	 * present, the data (message) is passed to the listener as a mutable
	 * object.
	 * 
	 * @param listener
	 */
	public void addListener(CommListener listener) {
		if (listener == null)
			return;

		listeners.putIfAbsent(listener.getListenerID(), listener);
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, eye.Comm.Request msg) throws Exception {
		System.out.println("ctx.channel().pipeline().toString()");
		for (String id : listeners.keySet()) {
			CommListener cl = listeners.get(id);

			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			cl.onMessage(msg);
		}
	}
}
