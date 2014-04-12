package poke.server.management.managers;

//import poke.client.comm.CommHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
/**
 * 
 * @author Amit
 */

public class JobConnHandler extends SimpleChannelInboundHandler<eye.Comm.Management>{
	JobConnector jobConnector = null;
	public JobConnHandler(JobConnector jc)
	{
		this.jobConnector = jc;
	}
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, eye.Comm.Management msg) throws Exception {
		//System.out.println("ctx.channel().pipeline().toString()");
		//As per current design there will no message here
		onMessage(msg);
	}
	

	private void onMessage(eye.Comm.Management msg)
	{
		if(jobConnector != null)
		{
			jobConnector.handleIncomingRequest(msg);
		}
	}
}

