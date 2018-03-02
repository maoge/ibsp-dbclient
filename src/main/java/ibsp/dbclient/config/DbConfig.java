package ibsp.dbclient.config;

import ibsp.dbclient.utils.PropertiesUtils;

public class DbConfig {
	
	private String configFile;
	
	public DbConfig(String configFile) {
		this.configFile = configFile;
		
		id = PropertiesUtils.getInstance(configFile).get("id");
		driver = PropertiesUtils.getInstance(configFile).get("driver");
		url = PropertiesUtils.getInstance(configFile).get("url");
		username = PropertiesUtils.getInstance(configFile).get("username");
		password = PropertiesUtils.getInstance(configFile).get("password");
		
		maxPoolSize = PropertiesUtils.getInstance(configFile).getInt("maxPoolSize", 10);
		minPoolSize = PropertiesUtils.getInstance(configFile).getInt("minPoolSize", 1);
		initPoolSize = PropertiesUtils.getInstance(configFile).getInt("initPoolSize", 5);
		maxIdleTime = PropertiesUtils.getInstance(configFile).getInt("maxIdleTime", 10*60*1000);
		maxLifetime = PropertiesUtils.getInstance(configFile).getInt("maxLifetime", 60*60*1000);  // 连接能存活的绝对时间
		connectionTimeout = PropertiesUtils.getInstance(configFile).getInt("connectionTimeout", 3000);  // 连接能存活的绝对时间
		validationTimeout = PropertiesUtils.getInstance(configFile).getInt("validationTimeout", 5000);
		idleConnectionTestPeriod = PropertiesUtils.getInstance(configFile).getInt("idleConnectionTestPeriod", 60000);
	
		isAutoCommit = PropertiesUtils.getInstance(configFile).getBoolean("isAutoCommit", false);
		connectionTestQuery = PropertiesUtils.getInstance(configFile).get("connectionTestQuery", "select 1 from dual");
	}

	private String id;
	private String driver;
	private String url;
	private String username;
	private String password;
	private String connectionTestQuery;
	
	private int maxPoolSize = 10;
	private int minPoolSize = 1;
	private int initPoolSize = 5;
	private int connectionTimeout = 3000;
	private int maxIdleTime = 10*60*1000;
	private int maxLifetime = 60*60*1000;  // 连接能存活的绝对时间
	private int validationTimeout = 5*1000;
	private int idleConnectionTestPeriod = 60*1000;
	
	private boolean isAutoCommit = false;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
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

	public void setAutoCommit(boolean autoCommit) {
		this.isAutoCommit = autoCommit;
	}

	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}

	public String getConnectionTestQuery() {
		return connectionTestQuery;
	}

	public void setConnectionTestQuery(String connectionTestQuery) {
		this.connectionTestQuery = connectionTestQuery;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

}
