package misc;

import java.net.UnknownHostException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import eye.Comm.JobDesc;
import eye.Comm.JobDesc.JobCode;
import eye.Comm.NameValueSet;
import eye.Comm.NameValueSet.NodeType;

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
	
	/**
	 * @author chitra
	 * @param namespace
	 * @param criteria
	 * @return
	 * If the job action is 4, then List all the jobs present in the DB with that Job ID
	 */
	
	public List<JobDesc> findJobs(String namespace, JobDesc criteria){
		
		List<JobDesc> listJobs = null;
		
		DBCollection collection = getJobsCollection();
		
		BasicDBObject findQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("Job ID", criteria.getJobId()));
		obj.add(new BasicDBObject("NameSpace", namespace));
		findQuery.put("$and", obj);
		
		System.out.println(findQuery.toString());
		
		DBCursor cursor = collection.find(findQuery);
		String nmspace;
		long ownerId;
		String Jobid;
		JobCode status;
		NodeType nodetype;
		NameValueSet.Builder nameval = NameValueSet.newBuilder();
		String name;
		String value;
		
		while(cursor.hasNext())
		{
			JobDesc.Builder jobs = JobDesc.newBuilder();
			nmspace = cursor.curr().get("NameSpace").toString();
			ownerId = (long)(cursor.curr().get("Owner ID"));
			Jobid = cursor.curr().get("Job ID").toString();
			status = (JobCode)cursor.curr().get("Job Status Code");
			nodetype = (NodeType)cursor.curr().get("Node Type");
			name = cursor.curr().get("Name").toString();
			value = cursor.curr().get("Value").toString();
			
			jobs.setNameSpace(nmspace);
			jobs.setOwnerId(ownerId);
			jobs.setJobId(Jobid);
			jobs.setStatus(status);
			nameval.setName(name);
			nameval.setValue(value);
			nameval.setNodeType(nodetype);
			jobs.setOptions(nameval);
			
			//adding to the JobDesc list
			listJobs.add(jobs.build());
		}
		 
		return listJobs;
	}

}
