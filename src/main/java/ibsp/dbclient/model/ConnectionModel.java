package ibsp.dbclient.model;

import ibsp.dbclient.config.DbConfig;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.exception.DBException.DBERRINFO;
import ibsp.dbclient.utils.DES3;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionModel {
	
	private static final Logger logger = LoggerFactory.getLogger(ConnectionModel.class);
	
	private boolean isOpen = false;
	private String testQuery;
	
	private HikariConfig hikariConf = null;
	private HikariDataSource dataSource = null;
	
	public ConnectionModel(String configFile) {
		DbConfig config = new DbConfig(configFile);
		
		hikariConf = new HikariConfig();
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
		
		initDataSource();
	}
	
	private boolean initDataSource() {
		try {
			dataSource = new HikariDataSource(hikariConf);
			isOpen = true;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		
		return isOpen;
	}
	
	public Connection getConnection() throws DBException {
		if (dataSource == null) {
			initDataSource();
		}
		
		Connection conn = null;
		try {
			if (dataSource != null) {
				conn = dataSource.getConnection();
			}
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new DBException(e.getMessage(), e, DBERRINFO.e2);
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
