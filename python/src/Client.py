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
from javax.net.ssl import SSLContext

class SSLInitializer(ChannelInitializer):
 
  def initChannel(self, ch):
    pipeline = ch.pipeline()
    engine = SSLContext.getDefault().createSSLEngine()
    engine.setUseClientMode(True);
    pipeline.addLast("ssl", SslHandler(engine))
        
class Client():

  def run(self, host, port):
  
    request = Request.getDefaultInstance()
    #header = request.header
    #body = request.body	  
    #header.routing_id = 4
    #header.originator = "zero"
    #job_op = body.job_op
    #job_op.action = 4
    
    group = NioEventLoopGroup()
    
    try:
      bootstrap = Bootstrap().group(group).channel(NioSocketChannel).handler(SSLInitializer()).option(ChannelOption.TCP_NODELAY, True).option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      channel = bootstrap.connect(host, port).sync().channel()
      channel.writeAndFlush(request).sync()
      channel.read()
    finally:
      print "Shutting down"
      group.shutdown()

    
