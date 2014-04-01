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


	public DBCollection getJobsCollection()
	{
		//Connect to Mongo DB..
		MongoClient mongo;
		DBCollection collection = null;
		try {
			mongo = new MongoClient("localhost", 27017);
			//Create DB..
			DB mongodb = mongo.getDB("nettify");
			collection = mongodb.getCollection("jobs");
		} catch (UnknownHostException e) {
			System.out.println("Exception in Connection to Mongo "+e);
		}
		catch(MongoException me) {
			me.printStackTrace();
		}
		return collection;
	}

	/**
	 * @author Chitra
	 * @param namespace
	 * @param job
	 * if the job action is 1 - Add Job then add the job desc to the DB
	 * @return
	 */

	public boolean addJob(String namespace, JobDesc job) {

		try {
			DBCollection collection = getJobsCollection();
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
		} 
		catch(MongoException me) {
			me.printStackTrace();
		}
		return false;
	}

	/**
	 * @author Chitra
	 * @param namespace
	 * @param jobId
	 * If the job action is 3 - remove job, then remove the job from the DB
	 * @return
	 */
	public boolean removeJob(String namespace, String jobId) {

		DBCollection collection = getJobsCollection();
		int count = 0;

		//fetch all the records with the same JobID and remove them
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("Job ID", jobId);
		DBCursor cursor = collection.find(whereQuery);
		while(cursor.hasNext())
		{
			collection.remove(whereQuery);
			count++;
		}
		if(count > 0)
			return true;
		else
			return false;
	}

}
