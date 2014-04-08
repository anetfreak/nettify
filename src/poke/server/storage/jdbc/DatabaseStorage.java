/*
 * copyright 2014, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import misc.MongoDao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.storage.Storage;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import eye.Comm.JobDesc;
import eye.Comm.NameSpace;

public class DatabaseStorage implements Storage {
	protected static Logger logger = LoggerFactory.getLogger("database");

	public static final String sDriver = "jdbc.driver";
	public static final String sUrl = "jdbc.url";
	public static final String sUser = "jdbc.user";
	public static final String sPass = "jdbc.password";

	protected Properties cfg;
	protected BoneCP cpool;

	MongoDao mongodb = new MongoDao();

	public DatabaseStorage() {
	}

	public DatabaseStorage(Properties cfg) {
		init(cfg);
	}

	@Override
	public void init(Properties cfg) {
		if (cpool != null)
			return;

		this.cfg = cfg;

		try {
			Class.forName(cfg.getProperty(sDriver));
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(cfg.getProperty(sUrl));
			config.setUsername(cfg.getProperty(sUser, "sa"));
			config.setPassword(cfg.getProperty(sPass, ""));
			config.setMinConnectionsPerPartition(5);
			config.setMaxConnectionsPerPartition(10);
			config.setPartitionCount(1);

			cpool = new BoneCP(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see gash.jdbc.repo.Repository#release()
	 */
	@Override
	public void release() {
		if (cpool == null)
			return;

		cpool.shutdown();
		cpool = null;
	}

	@Override
	public NameSpace getNameSpaceInfo(long spaceId) {
		NameSpace space = null;

		Connection conn = null;
		try {
			conn = cpool.getConnection();
			conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			// TODO complete code to retrieve through JDBC/SQL
			// select * from space where id = spaceId
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on looking up space " + spaceId, ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return space;
	}

	@Override
	public List<NameSpace> findNameSpaces(NameSpace criteria) {
		List<NameSpace> list = null;

		Connection conn = null;
		try {
			conn = cpool.getConnection();
			conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			// TODO complete code to search through JDBC/SQL
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on find", ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return list;
	}

	@Override
	public NameSpace createNameSpace(NameSpace space) {
		if (space == null)
			return space;

		Connection conn = null;
		try {
			conn = cpool.getConnection();
			conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			// TODO complete code to use JDBC
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on creating space " + space, ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}

			// indicate failure
			return null;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return space;
	}

	@Override
	public boolean removeNameSpace(long spaceId) {
		// TODO Auto-generated method stub
		return false;
	}


	/**
	 * Adding a new JobOperation to the MongoDB if the JobAction is 1 - AddJob
	 * @author Chitra
	 */
	@Override
	public boolean addJob(String namespace, JobDesc job) {
		if(mongodb.addJob(namespace, job))
			return true;
		else
			return false;
	}

	/**
	 * @author Chitra
	 * If the Job action is 3 - Remove Job, then Remove the job from the DB according to the Job ID
	 */

	@Override
	public boolean removeJob(String namespace, String jobId) {
		if(mongodb.removeJob(namespace, jobId))
			return true;
		else
			return false;
	}

	@Override
	public boolean updateJob(String namespace, JobDesc job) {
		return false;
	}

	/**
	 * @author Chitra
	 * If the job action is 4 - List Jobs, then list all the jobs associated with that Job ID from the DB
	 */
	@Override
	public List<JobDesc> findJobs(String namespace, JobDesc criteria) {
		List<JobDesc> listJobs = mongodb.findJobs(namespace, criteria);
			return listJobs;
	}

	/**
	 * @author chitra
	 * @return
	 * If a request is received for listcourses, then display all the courses from the database
	 */
	public List<String> listCourses() {
		List<String> courses = mongodb.listCourses();
		return courses;
	}
}
