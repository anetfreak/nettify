package misc;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import eye.Comm.JobDesc;

public class MongoDao {

	
	public boolean addJob(String namespace, JobDesc job) {
		
		try {
			//Connect to Mongo DB..
			MongoClient mongo = new MongoClient("localhost", 27017);
			
			//Create DB..
			DB mongodb = mongo.getDB("nettify");
			DBCollection collection = mongodb.getCollection("courses");
			
			//Insert..
			BasicDBObject doc = new BasicDBObject();
			doc.put("NameSpace", namespace);
			doc.put("Owner ID", job.getOwnerId());
			doc.put("Job ID", job.getJobId());
			doc.put("Job Status Code", job.getStatus().name());
			doc.put("Node Type", job.getOptions().getNodeType().getNumber());
			doc.put("Name", job.getOptions().getName());
			doc.put("Value", job.getOptions().getValue());
			collection.insert(doc);
			
			//Find and display..
			BasicDBObject query = new BasicDBObject().append("id", 1);
			DBCursor cursor = collection.find(query);
			while(cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			return true;
			
		} catch(UnknownHostException uhe) {
			uhe.printStackTrace();
		} catch(MongoException me) {
			me.printStackTrace();
		}
		return false;
	}

}
