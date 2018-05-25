package ibsp.dbclient.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibsp.dbclient.utils.CONSTS;
import ibsp.dbclient.utils.PropertiesUtils;

public class DbConfig {
	
	private static Logger logger = LoggerFactory.getLogger(DbConfig.class);
	
	private static DbConfig instance = null;
	private static Object mtx = null;
	
	private String metaSvrRootUrl;
	private String serviceID;
	
	private String driver = "com.mysql.jdbc.Driver";
	private String dbName;
	private String dbProperties; //something like characterEncoding=utf8&useSSL=true
	private String username;
	private String password;

	private int maxPoolSize = 10;
	private int minPoolSize = 1;
	private int initPoolSize = 5;
	private int connectionTimeout = 3000;
	private int maxIdleTime = 10*60*1000;
	private int maxLifetime = 60*60*1000;
	private int validationTimeout = 5*1000;
	private int idleConnectionTestPeriod = 60*1000;
	
	private boolean isAutoCommit = false;
	private String connectionTestQuery = "select 1 from dual";
	
	static {
		mtx = new Object();
	}
	
	public static DbConfig get() {
		if (instance != null)
			return instance;
		
		synchronized(mtx) {
			if (instance == null) {
				instance = new DbConfig();
			}
			
			return instance;
		}
	}
	
	public DbConfig() {
		PropertiesUtils prop = null;
		try {
			prop = PropertiesUtils.getInstance(CONSTS.INIT_PROP_FILE);
		} catch (Exception e) {
			logger.warn("load property file:{} error!", CONSTS.INIT_PROP_FILE);
			return;
		}
		
		this.metaSvrRootUrl = prop.get(CONSTS.METASVR_ROOTURL);
		this.serviceID      = prop.get(CONSTS.DB_SERVICE_ID);
		
		this.driver         = prop.get(CONSTS.DB_DRIVER);
		this.dbName         = prop.get(CONSTS.DB_NAME);
		this.dbProperties   = prop.get(CONSTS.DB_PROPS, "");
		this.username       = prop.get(CONSTS.DB_USERNAME, "");
		this.password       = prop.get(CONSTS.DB_PWD, "");
		
		this.maxPoolSize 	= prop.getInt(CONSTS.DB_MAX_POOL_SIZE, 10);
		this.minPoolSize 	= prop.getInt(CONSTS.DB_MIN_POOL_SIZE, 1);
		this.initPoolSize 	= prop.getInt(CONSTS.DB_INIT_POOL_SIZE, 5);
		this.maxIdleTime 	= prop.getInt(CONSTS.DB_MAX_IDLE_TIME, 10*60*1000);
		this.maxLifetime 	= prop.getInt(CONSTS.DB_MAX_LIFE_TIME, 60*60*1000);
		this.connectionTimeout = prop.getInt(CONSTS.DB_CONN_TIMEOUT, 3000);
		this.validationTimeout = prop.getInt(CONSTS.DB_VALIDATION_TIMEOUT, 5000);
		this.idleConnectionTestPeriod = prop.getInt(CONSTS.DB_IDLE_CONN_TEST_PERIOD, 60000);
		this.isAutoCommit = prop.getBoolean(CONSTS.DB_IS_AUTO_COMMIT, false);
		this.connectionTestQuery = prop.get(CONSTS.DB_CONN_TEST_QUERY, "select 1 from dual");
	}

	public String getMetaSvrRootUrl() {
		return metaSvrRootUrl;
	}

	public void setMetaSvrRootUrl(String metaSvrRootUrl) {
		this.metaSvrRootUrl = metaSvrRootUrl;
	}

	public String getServiceID() {
		return serviceID;
	}

	public void setServiceID(String serviceID) {
		this.serviceID = serviceID;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbProperties() {
		return dbProperties;
	}

	public void setDbProperties(String dbProperties) {
		this.dbProperties = dbProperties;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public int getMinPoolSize() {
		return minPoolSize;
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	public int getInitPoolSize() {
		return initPoolSize;
	}

	public void setInitPoolSize(int initPoolSize) {
		this.initPoolSize = initPoolSize;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public int getMaxIdleTime() {
		return maxIdleTime;
	}

	public void setMaxIdleTime(int maxIdleTime) {
		this.maxIdleTime = maxIdleTime;
	}

	public int getMaxLifetime() {
		return maxLifetime;
	}

	public void setMaxLifetime(int maxLifetime) {
		this.maxLifetime = maxLifetime;
	}

	public int getValidationTimeout() {
		return validationTimeout;
	}

	public void setValidationTimeout(int validationTimeout) {
		this.validationTimeout = validationTimeout;
	}

	public int getIdleConnectionTestPeriod() {
		return idleConnectionTestPeriod;
	}

	public void setIdleConnectionTestPeriod(int idleConnectionTestPeriod) {
		this.idleConnectionTestPeriod = idleConnectionTestPeriod;
	}

	public boolean isAutoCommit() {
		return isAutoCommit;
	}

	public void setAutoCommit(boolean isAutoCommit) {
		this.isAutoCommit = isAutoCommit;
	}

	public String getConnectionTestQuery() {
		return connectionTestQuery;
	}

	public void setConnectionTestQuery(String connectionTestQuery) {
		this.connectionTestQuery = connectionTestQuery;
	}
	
}
