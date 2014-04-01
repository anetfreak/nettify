from SocketChannel import SocketChannel, SocketChannelFactory
from comm_pb2 import Request, Header, Payload, RoutingPath

class PyClient():
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
      job_op.job_id = "zero"
      
      self.channel.write(request.SerializeToString())