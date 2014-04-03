package poke.server;


import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.buffer.ByteBuf;

import com.google.protobuf.MessageLite;

public class ServerInitializer extends ChannelInitializer<SocketChannel> {
	boolean compress = false;

	public ServerInitializer(boolean enableCompression) {
		compress = enableCompression;
	}
	
	public class my_lineframedecoder extends LengthFieldBasedFrameDecoder{
		
		public my_lineframedecoder(
	            int maxFrameLength,
	            int lengthFieldOffset, int lengthFieldLength,
	            int lengthAdjustment, int initialBytesToStrip)
	            {
	            	super(maxFrameLength,lengthFieldOffset,lengthFieldLength,lengthAdjustment,initialBytesToStrip);
	            }
		@Override
	    protected Object decode(
	            ChannelHandlerContext ctx, ByteBuf arg1) throws Exception {
				final int length = arg1.readableBytes();
				System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&& Length in frame decoder: "+length);
				System.out.println("Buffer: " + arg1.getUnsignedInt(4));
				return super.decode(ctx, arg1);
		}
	}
	public class my_protpbufDecoder extends ProtobufDecoder{

		public my_protpbufDecoder(MessageLite prototype) {
			super(prototype);
			// TODO Auto-generated constructor stub
		}
		
		   @Override
		    protected void decode(
		            ChannelHandlerContext ctx, ByteBuf arg1, List<Object> msg) throws Exception {
		        //if (!(msg instanceof ChannelBuffer)) {
		         //   return msg;
		        //}
			   final int length = arg1.readableBytes();
			   System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&& Length: "+length);
		        super.decode(ctx,arg1,msg);
		        //ChannelBuffer buf = (ChannByteBuf::belBuffer) msg;
		        
		    }
		
	}
	@Override
	public void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
		System.out.println("$$$$$$$$$$$$$$$$$$$$$ in InitChannel");
		// Enable stream compression (you can remove these two if unnecessary)
		if (compress) {
			pipeline.addLast("deflater", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
			pipeline.addLast("inflater", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
		}

		/**
		 * length (4 bytes).
		 * 
		 * Note: max message size is 64 Mb = 67108864 bytes this defines a
		 * framer with a max of 64 Mb message, 4 bytes are the length, and strip
		 * 4 bytes
		 */
		
		
		pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4));

		// pipeline.addLast("frameDecoder", new
		// DebugFrameDecoder(67108864, 0, 4, 0, 4));

		// decoder must be first
		
		pipeline.addLast("protobufDecoder", new ProtobufDecoder(eye.Comm.Request.getDefaultInstance()));
		pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
		pipeline.addLast("protobufEncoder", new ProtobufEncoder());
		
		// our server processor (new instance for each connection)
		pipeline.addLast("handler", new ServerHandler());
	}
}
