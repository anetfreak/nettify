import sys
sys.path.append('/usr/lib')
from PyClient import PyClient

if __name__ == '__main__':
  import sys
  
  #Display the menu to the user. Perform the necessary action as per his choice.. 
  running = True
  while running:
      print "\n   ====================="
      print "   Welcome to Open MOOC!"
      print "   =====================\n"
      print "Choose one of the options below:"
      print "1. Basic Ping Request to this MOOC"
      print "2. List Courses on this MOOC"
      print "3. Get Description for a course"
      print "4. Get More Courses from other MOOC's"
      print "5. Quit"
      
      choice = int(input("\nYour Option -> "))
      if choice == 1:
          #Sending Ping Request to Server
          requestType = "ping"
          host = str(input("Enter host: "))
          port = int(input("Enter port: "))
          PyClient().chooseOperation(requestType, host, port)
      elif choice == 2:
          #Sending Ping Request to Server
          requestType = "listcourses"
          host = str(input("Enter host: "))
          port = int(input("Enter port: "))
          PyClient().chooseOperation(requestType, host, port)
      elif choice == 3:
          #Sending Ping Request to Server
          requestType = "getdescription"
          host = str(input("Enter host: "))
          port = int(input("Enter port: "))
          PyClient().chooseOperation(requestType, host, port)
      elif choice == 4:
          #Sending Ping Request to Server
          requestType = "getmorecourses"
          host = str(input("Enter host: "))
          port = int(input("Enter port: "))
          PyClient().chooseOperation(requestType, host, port)
      elif choice == 5:
          print "Bye!"
          running = False
      else:
          print "Please choose a valid option. -> "