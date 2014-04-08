package misc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ExtNodeConfig {

	public static void main(String[] args) {
		
		boolean displayMenu = true;
		while(displayMenu) {
			System.out.println("\n External Nodes Configuration..\n");
			System.out.println("1. Add a new external node");
			System.out.println("2. Update an external node");
			System.out.println("3. Display all external nodes");
//			System.out.println("4. Exit");
			
			System.out.println("\nEnter your choice -> ");
			
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			
			MongoDao mongoDao = new MongoDao();
			
			try {
				int input = Integer.parseInt(br.readLine());
				switch(input) {
					case 1:
						System.out.println("Node Name: ");
						String name = br.readLine();
						System.out.println("IP: ");
						String ip = br.readLine();
						System.out.println("Port: ");
						Integer port = Integer.parseInt(br.readLine());
						System.out.println("Management Port: ");
						Integer mgmtPort = Integer.parseInt(br.readLine());
						
						if(ip != null && port != null && mgmtPort != null && name != null) {
							ExtNode node = new ExtNode();
							node.setIp(ip);
							node.setName(name);
							node.setPort(port);
							node.setMgmtPort(mgmtPort);
							
							mongoDao.addExtNode(node);
							
						} else {
							System.out.println("Invalid Input. Please specify all the fields properly.");
						}
					
					case 3:
						ArrayList<ExtNode> extNodes = mongoDao.findExtNodes();
						for (ExtNode eNode: extNodes) {
							System.out.println("\nNode name : " + eNode.getName());
							System.out.println("Node IP : " + eNode.getIp());
							System.out.println("Node Port : " + eNode.getPort());
							System.out.println("Node Management Port : " + eNode.getMgmtPort() + "\n");
						}
					
					case 4:
//						displayMenu = false;
						
				}
				
				
			} catch (IOException ioe) {
				System.out.println("IO error trying to read your name!");
				System.exit(1);
			}
		}
	}

}
