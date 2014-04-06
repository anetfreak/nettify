from SocketChannel import SocketChannel, SocketChannelFactory
from comm_pb2 import Request, Header, Payload, RoutingPath
import struct
import sys

class PyClient():
  def __init__(self):
    self.channelFactory = SocketChannelFactory()
  
  def chooseOperation(self, choice, host, port):
      if choice == "ping":
          #Form Ping request here and send it to the server.
          request = self.formPingRequest()
          print "Preparing to send ping request to server <" + str(host) + ":" + str(port) +">"
          response = self.run(host, port, request)
          self.printPingRequest(response)
      elif choice == "listcourses":
          #Form Job request here and send it to the server.
          print "Preparing to send job request to list courses to server <" + str(host) + ":" + str(port) +">"
      elif choice == "getdescription":
          #Form Job request here and send it to the server.
          print "Preparing to send job request to get course description to server <" + str(host) + ":" + str(port) +">"
      elif choice == "getmorecourses":
          #Form Job request here and send it to the server. The server should take care of sending this to the external MOOC's
          print "Preparing to send job request to get other MOOC's courses to server <" + str(host) + ":" + str(port) +">"
  
  def formPingRequest(self):
      request = Request()
      header = request.header
      body = request.body
      
      header.routing_id = 2
      header.originator = "zero"
      ping = body.ping
      ping.number = 4
      ping.tag = "zero"
      return request
  
  def printPingRequest(self, resp):
      print "\n==Response Received from Server==\n"
      print "RoutingID - " + str(resp.header.routing_id)
      print "Originator - " + str(resp.header.originator)
      print "Ping Number - " + str(resp.body.ping.number)
      print "Ping Tag - " + str(resp.body.ping.tag)
                      
  def run(self, host, port, request):
    self.channel = self.channelFactory.openChannel(host, port)
    while self.channel.connected:
      print "Channel Connected..."
      try:
          self.channel.write(request.SerializeToString())
          resp = Request()
          resp.ParseFromString(self.channel.read())
          return resp
      except:
        print sys.exc_info()[0]
      finally:
        self.channel.close()  
