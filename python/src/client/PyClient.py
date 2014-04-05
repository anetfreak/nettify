from SocketChannel import SocketChannel, SocketChannelFactory
from comm_pb2 import Request, Header, Payload, RoutingPath
import struct
import sys

class PyClient():
  def __init__(self):
    self.channelFactory = SocketChannelFactory()
    
  def run(self, host, port):
    self.channel = self.channelFactory.openChannel(host, port)
    while self.channel.connected:
      print "Channel Connected..."
      try:
          request = Request()
          header = request.header
          body = request.body
          
          header.routing_id = 2
          header.originator = "zero"
          ping = body.ping
          ping.number = 4
          ping.tag = "zero"
    
          self.channel.write(request.SerializeToString())
          resp = Request()
          resp.ParseFromString(self.channel.read())
          print "\n==Response Received from Server==\n"
          print "RoutingID - " + str(resp.header.routing_id)
          print "Originator - " + str(resp.header.originator)
          print "Ping Number - " + str(resp.body.ping.number)
          print "Ping Tag - " + str(resp.body.ping.tag)
          
      except:
        print sys.exc_info()[0]
          
      finally:
        self.channel.close()  
