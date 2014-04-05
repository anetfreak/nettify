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
    
          message = request.SerializeToString()
          self.channel.write(struct.pack(str(len(message)) + "s", message))
          
          resp = Request()
          print resp.ParseFromString(self.channel.read())
          
      except:
        print sys.exc_info()[0]
          
      finally:
        self.channel.close()  
