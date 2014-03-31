from CommConnection import CommConnection
        
class Client():

  def run(self, host, port):
#     CommConnection(host, port).poke()
    CommConnection(host, port).addJobReq()