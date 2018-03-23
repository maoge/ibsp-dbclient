package ibsp.dbclient;

import ibsp.dbclient.config.DbConfig;
import ibsp.dbclient.config.MetasvrUrlConfig;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.exception.DBException.DBERRINFO;
import ibsp.dbclient.pool.ConnectionPool;
import ibsp.dbclient.pool.DbPoolImpl;
import ibsp.dbclient.utils.CONSTS;
import ibsp.dbclient.utils.HttpUtils;
import ibsp.dbclient.utils.SVarObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class DbSource {
	
	private static Logger logger = LoggerFactory.getLogger(DbSource.class);

	private static DbSource theInstance = null;
	
	private static Object mtx = new Object();
	private ConcurrentHashMap<String, ConnectionPool> validDBMap = null;
	private ConcurrentHashMap<String, ConnectionPool> invalidDBMap = null;
	private List<String> validIdList;
	private List<String> invalidIdList;
	private long index = 0L;
	
	private static Thread checkThread = null;
	private static DBPoolRecoveryChecker checker = null;
	private static volatile boolean isCheckerRunning = false;
	
	private static long DBPOOL_RECONNECT_INTERVAL = 1000L;
	
	private static MetasvrUrlConfig metaUrls; //metaserver address
	
	
	public DbSource() {
		validDBMap = new ConcurrentHashMap<String, ConnectionPool>();
		invalidDBMap = new ConcurrentHashMap<String, ConnectionPool>();
		validIdList = new ArrayList<String>();
		invalidIdList = new ArrayList<String>();
	}
	
	public static DbSource get() throws DBException {
		if (theInstance != null) {
			return theInstance;
		}
		
		synchronized(mtx) {
			if (theInstance == null) {
				theInstance = new DbSource();
				
				//process metaserver addresses
				String rootUrls = DbConfig.get().getMetaSvrRootUrl();
				metaUrls = new MetasvrUrlConfig(rootUrls);

				//process tidb addresses
				String[] dbAddress = null;
				String serviceID = DbConfig.get().getServiceID();
				if (serviceID==null || serviceID.isEmpty()) {
					throw new DBException("serviceID is empty!", new Throwable(), DBERRINFO.e1);
				}
				String initUrl = String.format("%s/%s/%s?%s", metaUrls.getNextUrl(), 
						CONSTS.TIDB_SERVICE, CONSTS.FUN_GET_ADDRESS, "SERV_ID="+DbConfig.get().getServiceID());
				SVarObject sVarInvoke = new SVarObject();
				boolean retInvoke = HttpUtils.getData(initUrl, sVarInvoke);
				if (retInvoke) {
					JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
					if (jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE) == CONSTS.REVOKE_OK) {
						dbAddress = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO).split(",");
					}
				}
				if (dbAddress==null || dbAddress.length==0) {
					throw new DBException("TIDB address url is empty!", new Throwable(), DBERRINFO.e1);
				}
				
				//init hikari pools
				for (int i=0; i<dbAddress.length; i++) {
					String id = "TIDB"+(i+1);
					DbPoolImpl connPool = new DbPoolImpl(id, dbAddress[i]);

					if (connPool.check()) {
						theInstance.validDBMap.put(id, connPool);
						theInstance.validIdList.add(id);
					} else {
						theInstance.invalidDBMap.put(id, connPool);
						theInstance.invalidIdList.add(id);
							
						startChecker();
					}
				}
			}
		}
		
		return theInstance;
	}
	
	public static ConnectionPool getPool() throws DBException {
		DbSource dbsource = DbSource.get();
		if (dbsource.validIdList.size() == 0) {
			throw new DBException("db source is empty", new Throwable(), DBERRINFO.e1);
		}
		
		ConnectionPool connPool = null;
		synchronized(mtx) {
			int seed = (int) (dbsource.index++ % dbsource.validIdList.size());
			String id = dbsource.validIdList.get(seed);
			connPool = dbsource.validDBMap.get(id);
		}
		
		return connPool;
	}
	
	public static void close() throws DBException {
		isCheckerRunning = false;
		stopChecker();
		
		DbSource dbsource = DbSource.get();
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
		}
	}
	
	public static void removeBrokenPool(String id) throws DBException {
		DbSource dbsource = DbSource.get();
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
	
	public static void mergeRecoveredPool(String id) throws DBException {
		DbSource dbsource = DbSource.get();
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
	
	public static void startChecker() {
		if (isCheckerRunning)
			return;
		
		isCheckerRunning = true;
		checker = new DBPoolRecoveryChecker();
		checkThread = new Thread(checker);
		checkThread.start();
	}
	
	public static void stopChecker() {
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
							DbSource.mergeRecoveredPool(id);
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
