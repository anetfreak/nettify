import sys
sys.path.append('/usr/lib')
from PyClient import PyClient

if __name__ == '__main__':
  import sys
  if len(sys.argv) != 3:
    print "Usage: python Main.py <host> <port>"
  else:
    host = sys.argv[1]
    port = int(sys.argv[2])
    print "Trying to connect to " + host + "..."
    PyClient().run(host, port)
