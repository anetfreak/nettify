package misc;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class MongoDao {

	public static void main(String[] args) {
		
		try {
			//Connect to Mongo DB..
			MongoClient mongo = new MongoClient("localhost", 27017);
			
			//Create DB..
			DB mongodb = mongo.getDB("nettify");
			DBCollection collection = mongodb.getCollection("courses");
			
			//Insert..
			BasicDBObject doc = new BasicDBObject();
			doc.put("id", 1);
			doc.put("name", "Distributed Systems");
			doc.put("university", "San Jose State University");
			doc.put("credits", 3);
			collection.insert(doc);
			
			//Find and display..
			BasicDBObject query = new BasicDBObject().append("id", 1);
			DBCursor cursor = collection.find(query);
			while(cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			
			
		} catch(UnknownHostException uhe) {
			uhe.printStackTrace();
		} catch(MongoException me) {
			me.printStackTrace();
		}

	}

}
