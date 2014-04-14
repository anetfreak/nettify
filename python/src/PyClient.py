from SocketChannel import SocketChannel, SocketChannelFactory
from comm_pb2 import Request, Header, Payload, RoutingPath, JobOperation
import struct, random, sys

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
          request = self.formJobRequest("listcourses")
          print "Preparing to send job request to list courses to server <" + str(host) + ":" + str(port) +">"
          response = self.run(host, port, request)
          self.printJobRequest(response)
      elif choice == "getdescription":
          #Form Job request here and send it to the server.
          request = self.formJobRequest("getdescription")
          print "Preparing to send job request to get course description to server <" + str(host) + ":" + str(port) +">"
          response = self.run(host, port, request)
          self.printJobRequest(response)
      elif choice == "getmorecourses":
          #Form Job request here and send it to the server. The server should take care of sending this to the external MOOC's
          request = self.formJobRequest("getmorecourses")
          print "Preparing to send job request to get other MOOC's courses to server <" + str(host) + ":" + str(port) +">"
          response = self.run(host, port, request)
          self.printJobRequest(response)
      elif choice == "competition":
          #Form Job request here and send it to the server. The server should take care of sending this to the external MOOC's
          request = self.formJobRequest("competition")
          print "Preparing to send job request to get other MOOC's courses to server <" + str(host) + ":" + str(port) +">"
          response = self.run(host, port, request)
          self.printJobRequest(response)
          
  def sendMockJobProposal(self, ns, host, port):
      #Form Ping request here and send it to the server.
      request = self.formJobProposal(ns)
      print "Preparing to send mock job proposal to server <" + str(host) + ":" + str(port) +">"
      response = self.run(host, port, request)
      print "Received response"
#       self.printPingRequest(response)
  
  def formPingRequest(self):
      request = Request()
      header = request.header
      body = request.body
      
#       header.routing_id = Header.Routing.PING
      header.routing_id = 2
      header.originator = "client"
      ping = body.ping
      ping.number = 4
      ping.tag = "Ping test"
      return request
  
  def formJobRequest(self, type):
      request = Request()
      header = request.header
      body = request.body
      
#       header.routing_id = Header.Routing.JOBS
      header.routing_id = 4
      header.originator = "client"
      jobOp = body.job_op
#       jobOp.action = JobAction.ADDJOB
      jobOp.action = 1
      data = jobOp.data
      
      data.name_space = type
      data.owner_id = 0
      data.job_id = str(random.randint(1, 10000))
#       data.status = JobCode.JOBUNKNOWN
      data.status = 1  
      
      return request
  
  def formJobProposal(self, type):
      
      mgmt = Management()
      proposal = mgmt.job_propose
            
      proposal.name_space = type
      proposal.owner_id = 0
      proposal.job_id = str(random.randint(1, 10000))
      proposal.weight = 1  
      
      return proposal
  
  def printPingRequest(self, resp):
      print "\n==Response Received from Server==\n"
      print "RoutingID - " + str(resp.header.routing_id)
      print "Originator - " + str(resp.header.originator)
      print "Ping Number - " + str(resp.body.ping.number)
      print "Ping Tag - " + str(resp.body.ping.tag)
      
  def printJobRequest(self, resp):
      print "\n==Response Received from Server==\n"
      print "RoutingID - " + str(resp.header.routing_id)
      print "Originator - " + str(resp.header.originator)
      print "Job Id - " + str(resp.body.job_status.job_id)
      print "Status of job request - " + str(resp.body.job_status.status)
      print "State of the job  - " + str(resp.body.job_status.job_state)
      data = resp.body.job_status.data.options
      for course in data:
        print course.value
                      
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
