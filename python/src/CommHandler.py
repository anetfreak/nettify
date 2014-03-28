import sys
sys.path.append("netty-all-4.0.15.Final.jar")
sys.path.append("protobuf-java-2.5.0.jar")
from io.netty.channel import Channel, ChannelFuture, ChannelPipeline, SimpleChannelInboundHandler
from eye.Comm import Request

class CommHandler(SimpleChannelInboundHandler):
    
    def setChannel(self, channel):
        self.channel = channel
        
    def send(self, message):
        if self.channel is None:
            print "Channel is None"
        else:
            cf = self.channel.writeAndFlush(message);
            cf.awaitUninterruptibly();
            print "In send ** Channel Handler** success"
            if cf.isDone() or cf.isSuccess():
                print "Done!"