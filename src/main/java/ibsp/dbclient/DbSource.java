package ibsp.dbclient;

import ibsp.common.utils.IBSPConfig;
import ibsp.dbclient.config.MetasvrConfigFactory;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.exception.DBException.DBERRINFO;
import ibsp.dbclient.pool.ConnectionPool;
import ibsp.dbclient.pool.DbPoolImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbSource {
	
	private static Logger logger = LoggerFactory.getLogger(DbSource.class);

	private static DbSource theInstance = null;
	private static Object mtx = new Object();
	
	private ConcurrentHashMap<String, ConnectionPool> validDBMap = null;
	private ConcurrentHashMap<String, ConnectionPool> invalidDBMap = null;
	private List<String> validIdList;
	private List<String> invalidIdList;
	private long index = 0L;
	
	private Thread checkThread = null;
	private DBPoolRecoveryChecker checker = null;
	private volatile boolean isCheckerRunning = false;
	
	private static long DBPOOL_RECONNECT_INTERVAL = 1000L;

	
	public static DbSource get() throws DBException {
		if (theInstance != null) {
			return theInstance;
		}
		
		synchronized(mtx) {
			if (theInstance == null) {
				theInstance = new DbSource();
			}
		}
		
		return theInstance;
	}

	public DbSource() throws DBException {
		validDBMap = new ConcurrentHashMap<String, ConnectionPool>();
		invalidDBMap = new ConcurrentHashMap<String, ConnectionPool>();
		validIdList = new ArrayList<String>();
		invalidIdList = new ArrayList<String>();
		
		String rootUrls = IBSPConfig.getInstance().getMetasvrUrl();
		MetasvrConfigFactory config = MetasvrConfigFactory.getInstance(rootUrls);
		
		//init hikari pools
		Map<String, String> dbAddress = config.getDbAddress();
		Set<Entry<String, String>> entrySet = dbAddress.entrySet();
		for (Entry<String, String> entry : entrySet) {
			this.addPool(entry.getKey(), entry.getValue());
		}
	}
	
	public void close() {
		isCheckerRunning = false;
		stopChecker();
		
		DbSource dbsource = theInstance;
		synchronized(mtx) {
			if (dbsource.validDBMap != null) {
				Set<Entry<String, ConnectionPool>> entrySet = dbsource.validDBMap.entrySet();
				for (Entry<String, ConnectionPool> entry : entrySet) {
					ConnectionPool connPool = entry.getValue();
					if (connPool == null) {
						continue;
					}
					try {
						connPool.close();
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
			
			if (dbsource.invalidDBMap != null) {
				Set<Entry<String, ConnectionPool>> entrySet = dbsource.invalidDBMap.entrySet();
				for (Entry<String, ConnectionPool> entry : entrySet) {
					ConnectionPool connPool = entry.getValue();
					if (connPool == null) {
						continue;
					}
					try {
						connPool.close();
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
			
			dbsource.validIdList.clear();
			dbsource.validDBMap.clear();
			
			dbsource.invalidIdList.clear();
			dbsource.invalidDBMap.clear();
			
			dbsource = null;
		}
	}
	
	public ConnectionPool getPool() throws DBException {
		if (validIdList.size() == 0) {
			throw new DBException("db source is empty", new Throwable(), DBERRINFO.e1);
		}
		
		ConnectionPool connPool = null;
		synchronized(mtx) {
			int seed = (int) (index++ % validIdList.size());
			String id = validIdList.get(seed);
			connPool = validDBMap.get(id);
		}
		
		return connPool;
	}
	
	public DataSource getDataSource() throws DBException {
		ConnectionPool connPool = getPool();
		if (connPool == null)
			return null;
		
		return connPool.getDataSource();
	}
	
	public void removeBrokenPool(String id) {
		DbSource dbsource = theInstance;
		synchronized(mtx) {
			if (dbsource.validDBMap.containsKey(id)) {
				dbsource.validIdList.remove(id);
				ConnectionPool connPool = dbsource.validDBMap.remove(id);
				if (connPool != null) {
					if (dbsource.invalidDBMap.containsKey(id)) {
						return;
					}
					
					if (!dbsource.invalidIdList.contains(id)) {
						logger.info("db pool:{} broken ......", id);
						dbsource.invalidDBMap.put(id, connPool);
						dbsource.invalidIdList.add(id);
					}
					
					if (!dbsource.invalidDBMap.isEmpty()) {
						startChecker();
					}
				}
			}
		}
	}
	
	public void mergeRecoveredPool(String id) {
		DbSource dbsource = theInstance;
		synchronized(mtx) {
			if (dbsource.invalidDBMap.containsKey(id)) {
				dbsource.invalidIdList.remove(id);
				ConnectionPool connPool = dbsource.invalidDBMap.remove(id);
				if (connPool != null) {
					if (dbsource.validDBMap.containsKey(id))
						return;
					
					if (!dbsource.validIdList.contains(id)) {
						logger.info("db pool:{} recovered ......", id);
						dbsource.validDBMap.put(id, connPool);
						dbsource.validIdList.add(id);
					}
					
					if (dbsource.invalidDBMap.isEmpty()) {
						stopChecker();
					}
				}
			}
		}
	}
	
	public void addPool(String id, String address) {
		DbPoolImpl connPool = new DbPoolImpl(id, address);
		System.out.println("Create pool to "+address);
		
		if (connPool.check()) {
			synchronized(mtx) {
				this.validDBMap.put(id, connPool);
				this.validIdList.add(id);
			}
		} else {
			synchronized(mtx) {
				this.invalidDBMap.put(id, connPool);
				this.invalidIdList.add(id);
			}
			startChecker();
		}
	}
	
	public void removePool(String id) throws Exception {
		System.out.println("Remove pool to "+id);
		ConnectionPool pool = null;
		synchronized(mtx) {
			if (this.validIdList.contains(id)) {
				pool = this.validDBMap.get(id);
				this.validDBMap.remove(id);
				this.validIdList.remove(id);
			} else if (this.invalidIdList.contains(id)) {
				pool = this.invalidDBMap.get(id);
				this.invalidDBMap.remove(id);
				this.invalidIdList.remove(id);
			}
		}
		if (pool != null) {
			pool.close();
		}
	}
	
	
	private void startChecker() {
		if (isCheckerRunning)
			return;
		
		isCheckerRunning = true;
		checker = new DBPoolRecoveryChecker();
		checkThread = new Thread(checker);
		checkThread.setName("DBPoolRecoveryChecker Thread");
		checkThread.setDaemon(true);
		checkThread.start();
	}
	
	private void stopChecker() {
		if (!isCheckerRunning)
			return;
		
		isCheckerRunning = false;
		if (checker != null) {
			checker.stopRunning();
			
			checker = null;
			checkThread = null;
		}
	}
	
	private static class DBPoolRecoveryChecker implements Runnable {
		
		private volatile boolean running = false;
		
		public DBPoolRecoveryChecker() {
			
		}

		@Override
		public void run() {
			running = true;
			
			while (running) {
				try {
					DbSource dbsource = DbSource.get();
					for (int i=0; i<dbsource.invalidIdList.size(); i++) {
						String id = dbsource.invalidIdList.get(i);
						logger.info("DBSource Checking:{} ......", id);
						DbPoolImpl connPool = (DbPoolImpl)dbsource.invalidDBMap.get(id);
						if (connPool.check()) {
							DbSource.get().mergeRecoveredPool(id);
						}
					}
				} catch (Exception e) {
					logger.warn("DBSource check error...", e);
				}
				
				try {
					Thread.sleep(DBPOOL_RECONNECT_INTERVAL);
				} catch (InterruptedException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		public void stopRunning() {
			running = false;
		}
		
	}
	
}
