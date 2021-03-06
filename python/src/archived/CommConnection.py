import sys

sys.path.append("netty-all-4.0.15.Final.jar")
sys.path.append("protobuf-java-2.5.0.jar")

from eye.Comm import Request, Header, Payload, RoutingPath, Ping, JobOperation, JobDesc
from com.google.protobuf import GeneratedMessage
from io.netty.bootstrap import Bootstrap, ChannelFactory
from io.netty.buffer import PooledByteBufAllocator, Unpooled
from io.netty.channel.nio import NioEventLoopGroup
from io.netty.channel.socket.nio import NioSocketChannel
from io.netty.channel import Channel, ChannelPipeline, SimpleChannelInboundHandler, ChannelOption
from io.netty.handler.codec import LengthFieldBasedFrameDecoder, LengthFieldPrepender
from io.netty.handler.codec.protobuf import ProtobufDecoder, ProtobufEncoder
from CommHandler import CommHandler
import jarray

class CommConnection():
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
        group = NioEventLoopGroup()
        try:
            #Create the eventLoopGroup and the channel
            self.handler = CommHandler(self)
            bootstrap = Bootstrap().group(group).channel(NioSocketChannel).handler(self.handler)
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,10000)
            bootstrap.option(ChannelOption.TCP_NODELAY, True)
            bootstrap.option(ChannelOption.SO_KEEPALIVE, True) 
            #bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            channel = bootstrap.connect(host, port).syncUninterruptibly()
            channel.awaitUninterruptibly(5000)
            if channel is None:
                print "Could not connect to the Server"
            else:
                print "Channel created" 
            
            pipeline = channel.channel().pipeline()
            pipeline.addLast("frameDecoder", LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4))
            pipeline.addLast("protobufDecoder", ProtobufDecoder(Request.getDefaultInstance()))
            pipeline.addLast("frameEncoder", LengthFieldPrepender(4))
            pipeline.addLast("protobufEncoder", ProtobufEncoder())    
            pipeline.addLast("handler", CommHandler(self))
            
            self.handler.setChannel(channel.channel())
        except:
            print sys.exc_info()[0]
        finally:
            print "finally!!"
            group.shutdownGracefully()
    
    def poke(self):
        ping = Ping.newBuilder()
        ping.setTag("Poke sample")
        ping.setNumber(5)
        
        #Payload
        r = Request.newBuilder()
        p = Payload.newBuilder()
        p.setPing(ping.build())
        r.setBody(p.build())

        #header with routing info
        h = Header.newBuilder()
        h.setOriginator("Node Zero")
        h.setTag("Poke")
        h.setRoutingId(Header.Routing.PING)
        r.setHeader(h.build())

        req = r.build()
        self.handler.send(req)
        
    def addJobReq(self):
        jobOp = JobOperation.newBuilder()
        jobOp.setAction(JobOperation.JobAction.ADDJOB)
        jobOp.setJobId("zero")
        
        jobDesc = JobDesc.newBuilder()
        jobDesc.setNameSpace("engineering")
        jobDesc.setOwnerId(0)
        jobDesc.setJobId("zero")
        jobDesc.setStatus(JobDesc.JobCode.JOBUNKNOWN)
        
        #Payload
        r = Request.newBuilder()
        p = Payload.newBuilder()
        jobOp.setData(jobDesc.build())
        p.setJobOp(jobOp.build())
        r.setBody(p.build())

        #header with routing info
        h = Header.newBuilder()
        h.setOriginator("Node Zero")
        h.setTag("Request to add a new job")
        h.setRoutingId(Header.Routing.JOBS)
        r.setHeader(h.build())

        req = r.build()
        self.handler.send(req)
        
    def listJobsReq(self):
        jobOp = JobOperation.newBuilder()
        jobOp.setAction(JobOperation.JobAction.LISTJOBS)
        jobOp.setJobId("zero")
        
        #Payload
        r = Request.newBuilder()
        p = Payload.newBuilder()
        p.setJobOp(jobOp.build())
        r.setBody(p.build())

        #header with routing info
        h = Header.newBuilder()
        h.setOriginator("Node Zero")
        h.setTag("Request to List all jobs")
        h.setRoutingId(Header.Routing.JOBS)
        r.setHeader(h.build())

        req = r.build()
        self.handler.send(req)
        
    def removeJobReq(self):
        jobOp = JobOperation.newBuilder()
        jobOp.setAction(JobOperation.JobAction.REMOVEJOB)
        jobOp.setJobId("zero")
        
        #Payload
        r = Request.newBuilder()
        p = Payload.newBuilder()
        p.setJobOp(jobOp.build())
        r.setBody(p.build())

        #header with routing info
        h = Header.newBuilder()
        h.setOriginator("Node Zero")
        h.setTag("Request to remove job")
        h.setRoutingId(Header.Routing.JOBS)
        r.setHeader(h.build())

        req = r.build()
        self.handler.send(req)
        
    def onMessage(self, msg):
        print "Inside onMessage"
        print "Printing Header of the message"
        length = msg.writerIndex()
        data = [None]
        data[0] = buf = jarray.zeros(length, "b")
        msg.getBytes(0, buf)
        print data[0].tostring()
      
    def printHeader(h):
        print "Header"
        print " - Orig   : " + h.getOriginator()
        print " - Req ID : " + h.getRoutingId()
        print " - Tag    : " + h.getTag()
        print " - Time   : " + h.getTime()
        print ""    
                    
