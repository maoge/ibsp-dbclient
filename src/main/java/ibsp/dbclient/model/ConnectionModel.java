package ibsp.dbclient.model;

import ibsp.dbclient.config.DbConfig;
import ibsp.dbclient.utils.DES3;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionModel {
	
	private static final Logger logger = LoggerFactory.getLogger(ConnectionModel.class);
	
	private String configFile;
	private boolean isOpen = false;
	private String testQuery;
	
	private HikariDataSource dataSource = null;
	
	public ConnectionModel(String configFile) {
		this.configFile = configFile;
		initDataSource();
	}
	
	private boolean initDataSource() {
		DbConfig config = new DbConfig(configFile);
		
		HikariConfig hikariConf = new HikariConfig();
		hikariConf.setPoolName(config.getId());
		hikariConf.setDriverClassName(config.getDriver());
		hikariConf.setJdbcUrl(config.getUrl());
		hikariConf.setUsername(config.getUsername());
		hikariConf.setPassword(DES3.decrypt(config.getPassword()));
		hikariConf.setMaximumPoolSize(config.getMaxPoolSize());
		hikariConf.setMinimumIdle(config.getMinPoolSize());
		
		hikariConf.setConnectionTimeout(config.getConnectionTimeout());
		hikariConf.setIdleTimeout(config.getMaxIdleTime());
		hikariConf.setMaxLifetime(config.getMaxLifetime());
		hikariConf.setValidationTimeout(config.getValidationTimeout());
		hikariConf.setLeakDetectionThreshold(config.getIdleConnectionTestPeriod());
		hikariConf.setConnectionTestQuery(config.getConnectionTestQuery());
		hikariConf.setAutoCommit(config.isAutoCommit());
		
		testQuery = config.getConnectionTestQuery();
		dataSource = new HikariDataSource(hikariConf);
		isOpen = true;
		
		return isOpen;
	}
	
	public Connection getConnection() throws SQLException, PropertyVetoException {
		if (dataSource == null) {
			initDataSource();
		}
		
		Connection conn = null;
		try {
		if (dataSource != null) {
			conn = dataSource.getConnection();
		}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return conn;
	}
	
	public Connection newConnection() {
		Connection conn = null;
		try {
			return getConnection();
		} catch (Exception ex) {
			logger.error("Can not create a connection to " + dataSource.getJdbcUrl(), ex);
		}		
		return conn;
	}
	
	public String getTestQuery() {
		return testQuery;
	}
	
	public void close() throws SQLException {
		if (dataSource != null && isOpen) {
			dataSource.close();
			isOpen = false;
		}
	}

}
