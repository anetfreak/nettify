from SocketChannel import SocketChannel, SocketChannelFactory

class Client():
  def __init__(self):
    self.channelFactory = SocketChannelFactory()
	
  def run(self, host, port):
	self.channel = self.channelFactory.openChannel(host, port)
	while self.channel.connected:
	  print "Channel Connected..."
