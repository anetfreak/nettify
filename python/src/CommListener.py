import sys
sys.path.append("netty-all-4.0.15.Final.jar")
sys.path.append("protobuf-java-2.5.0.jar")
from eye.Comm import Request, Header, Payload, RoutingPath, Ping, JobOperation, JobDesc
        
class CommListener():
    
    def onMessage(self, msg):
        print "Inside onMessage"
        print "Printing Header of the message"
        printHeader(msg.getHeader())
      
    def printHeader(h):
        print "Header"
        print " - Orig   : " + h.getOriginator()
        print " - Req ID : " + h.getRoutingId()
        print " - Tag    : " + h.getTag()
        print " - Time   : " + h.getTime()
        print " - Status : " + h.getReplyCode()
        if h.getReplyCode().getNumber() != eye.Comm.PokeStatus.SUCCESS_VALUE:
            print " - Re Msg : " + h.getReplyMsg()

        print ""