import sys
sys.path.append('/usr/lib')
from Client import Client
if __name__ == '__main__':
  import sys
  if len(sys.argv) != 3:
    print "Usage: python main.py <host> <port>"
  else:
    host = sys.argv[1]
    port = int(sys.argv[2])
    print "Trying to connect to " + host + "..."
    Client().run(host, port)
