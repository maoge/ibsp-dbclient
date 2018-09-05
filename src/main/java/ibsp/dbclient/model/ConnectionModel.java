package ibsp.dbclient.model;

import ibsp.common.utils.CONSTS;
import ibsp.common.utils.DES3;
import ibsp.common.utils.IBSPConfig;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.exception.DBException.DBERRINFO;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

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
	
	public ConnectionModel(String id, String address) {
		IBSPConfig properties = IBSPConfig.getInstance();
		String url = CONSTS.JDBC_HEADER+address+"/"+properties.getDbName();
		if (!properties.getDbProperties().isEmpty()) {
			url += "?"+properties.getDbProperties();
		}
		
		hikariConf = new HikariConfig();
		hikariConf.setPoolName(id);
		hikariConf.setDriverClassName(properties.getDbDriver());
		hikariConf.setJdbcUrl(url);
		hikariConf.setUsername(properties.getDbUsername());
		hikariConf.setPassword(DES3.decrypt(properties.getDbPassword()));
		hikariConf.setMaximumPoolSize(properties.getDbMaxPoolSize());
		hikariConf.setMinimumIdle(properties.getDbMinPoolSize());
		
		hikariConf.setConnectionTimeout(properties.getDbConnectionTimeout());
		hikariConf.setIdleTimeout(properties.getDbMaxIdleTime());
		hikariConf.setMaxLifetime(properties.getDbMaxLifetime());
		hikariConf.setValidationTimeout(properties.getDbValidationTimeout());
		hikariConf.setLeakDetectionThreshold(properties.getDbIdleConnectionTestPeriod());
		hikariConf.setConnectionTestQuery(properties.getDbConnectionTestQuery());
		hikariConf.setAutoCommit(properties.DbIsAutoCommit());
		
		hikariConf.setLeakDetectionThreshold(300000);
		
		testQuery = properties.getDbConnectionTestQuery();
		
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
	
	public DataSource getDataSource() {
		return dataSource;
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
