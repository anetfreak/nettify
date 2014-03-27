import sys
sys.path.append("netty-all-4.0.15.Final.jar")
sys.path.append("protobuf-java-2.5.0.jar")
from eye.Comm import Request, Header, Payload, RoutingPath
from io.netty.bootstrap import Bootstrap, ChannelFactory
from io.netty.buffer import PooledByteBufAllocator, Unpooled
from io.netty.channel import ChannelInboundHandlerAdapter, ChannelInitializer, ChannelOption
from io.netty.channel.nio import NioEventLoopGroup
from io.netty.channel.socket.nio import NioSocketChannel
from io.netty.handler.ssl import SslHandler
class Client():
  def __init__(self):
    self.channelFactory = SocketChannelFactory()
	
  def run(self, host, port):
	self.channel = self.channelFactory.openChannel(host, port)
	while self.channel.connected:
	  print "Channel Connected..."
	  request = Request()
	  header = request.header
	  body = request.body
	  
	  header.routing_id = 4
	  header.originator = "zero"
	  job_op = body.job_op
	  job_op.action = 4
	  
	  self.channel.write(request.SerializeToString())
	  print "Writing " + request.SerializeToString() + " to queue"
	  continue
