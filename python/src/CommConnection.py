import sys

sys.path.append("netty-all-4.0.15.Final.jar")
sys.path.append("protobuf-java-2.5.0.jar")

from eye.Comm import Request, Header, Payload, RoutingPath, Ping, JobOperation
from com.google.protobuf import GeneratedMessage
from io.netty.bootstrap import Bootstrap, ChannelFactory
from io.netty.buffer import PooledByteBufAllocator, Unpooled
from io.netty.channel.nio import NioEventLoopGroup
from io.netty.channel.socket.nio import NioSocketChannel
from io.netty.channel import Channel, ChannelPipeline, SimpleChannelInboundHandler, ChannelOption
from io.netty.handler.codec import LengthFieldBasedFrameDecoder, LengthFieldPrepender
from io.netty.handler.codec.protobuf import ProtobufDecoder, ProtobufEncoder
from CommHandler import CommHandler

class CommConnection():
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
        group = NioEventLoopGroup()
        try:
            #Create the eventLoopGroup and the channel
            self.handler = CommHandler()
            bootstrap = Bootstrap().group(group).channel(NioSocketChannel).handler(self.handler)
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,10000)
            bootstrap.option(ChannelOption.TCP_NODELAY, True)
            bootstrap.option(ChannelOption.SO_KEEPALIVE, True) 
            #bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            channel = bootstrap.connect(host, port).syncUninterruptibly()
            channel.awaitUninterruptibly(2000)
            if channel is None:
                print "Could not connect to the Server"
            else:
                print "Channel created" 
            
            pipeline = channel.channel().pipeline()
            pipeline.addLast("frameDecoder", LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4))
            pipeline.addLast("protobufDecoder", ProtobufDecoder(Request.getDefaultInstance()))
            pipeline.addLast("frameEncoder", LengthFieldPrepender(4))
            pipeline.addLast("protobufEncoder", ProtobufEncoder())    
            pipeline.addLast("handler", CommHandler())
            
            self.handler.setChannel(channel.channel())
        except:
            print sys.exc_info()[0]
        finally:
            print "finally!!"
            #group.shutdownGracefully()
    
    def poke(self):
        ping = Ping.newBuilder()
        ping.setTag("test poke")
        ping.setNumber(5)
        
        #Payload
        r = Request.newBuilder()
        p = Payload.newBuilder()
        p.setPing(ping.build())
        r.setBody(p.build())

        #header with routing info
        h = Header.newBuilder()
        h.setOriginator("client")
        h.setTag("test finger")
        h.setRoutingId(Header.Routing.PING)
        r.setHeader(h.build())

        req = r.build()
        self.handler.send(req)
        
    def jobrequest(self):
        ping = Ping.newBuilder()
        ping.setTag("test job")
        ping.setNumber(5)
        
        jobOp = JobOperation.newBuilder()
        jobOp.setAction(JobOperation.JobAction.LISTJOBS)
        
        #Payload
        r = Request.newBuilder()
        p = Payload.newBuilder()
        p.setPing(ping.build())
        p.setJobOp(jobOp.build())
        r.setBody(p.build())

        #header with routing info
        h = Header.newBuilder()
        h.setOriginator("client")
        h.setTag("test finger")
        h.setRoutingId(Header.Routing.JOBS)
        r.setHeader(h.build())

        req = r.build()
        self.handler.send(req)    
                    
