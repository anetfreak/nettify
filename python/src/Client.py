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

from CommConnection import CommConnection
#import jarray
#from threading import Condition

# class SSLInitializer(ChannelInitializer):
#  
#   def initChannel(self, ch):
#     pipeline = ch.pipeline()
#     engine = SSLContext.getDefault().createSSLEngine()
#     engine.setUseClientMode(True);
#     pipeline.addLast("ssl", SslHandler(engine))
        
class Client():

  def run(self, host, port):
      CommConnection(host, port).poke()
  
#     request = Request.getDefaultInstance()
    #header = request.header
    #body = request.body	  
    #header.routing_id = 4
    #header.originator = "zero"
    #job_op = body.job_op
    #job_op.action = 4
    
#     group = NioEventLoopGroup()
#     
#     try:
#       bootstrap = Bootstrap().group(group).channel(NioSocketChannel).handler(SSLInitializer()).option(ChannelOption.TCP_NODELAY, True).option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
#       channel = bootstrap.connect(host, port).sync().channel()
#       
#       data = [None]
#       #cv = Condition()
#       
#       class ReadAdapter(ChannelInboundHandlerAdapter):
#         def channelRead(self, ctx, msg):
#             try:
#                 length = msg.writerIndex()
#                 print "length=", length
#                 data[0] = buf = jarray.zeros(length, "b")
#                 msg.getBytes(0, buf)
#                 #cv.acquire()
#                 #cv.notify()
#                 #cv.release()
#             finally:
#                 msg.release()
#       
#       channel.pipeline().addLast(ReadAdapter())          
#       channel.writeAndFlush(Unpooled.wrappedBuffer(request)).sync()
#       channel.read()
#       #cv.acquire()
#       #while not data[0]:
#         #cv.wait()
#       #cv.release()
#       #channel.close().sync()
#       print data[0].tostring()
#       
#       
#     finally:
#       print "Shutting down"
#       group.shutdown()

    
