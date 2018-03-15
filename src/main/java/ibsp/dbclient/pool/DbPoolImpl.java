package ibsp.dbclient.pool;

import ibsp.dbclient.DbSource;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.model.ConnectionModel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbPoolImpl implements ConnectionPool {
	
	private static Logger logger = LoggerFactory.getLogger(DbPoolImpl.class);
	
	private String id;
	private ConnectionModel model;
	
	public DbPoolImpl(String id, String address) {
		this.id = id;
		this.model = new ConnectionModel(id, address);
	}

	@Override
	public void close() throws Exception {
		model.close();
	}
	
	@Override
	public Connection getConnection() {
		Connection conn = null;

		try {
			conn = model.getConnection();
		} catch (DBException e) {
			//no DBException will be thrown out here
			try {
				DbSource.removeBrokenPool(id);
			} catch (DBException e1) {}
		}
		
		return conn;
	}

	@Override
	public void recycle(Connection conn) {
		try {
			if (conn != null)
				conn.close();   // call close on connection to return to the pool.
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public String getName() {
		return id;
	}
	
	public boolean check() {
		Connection conn = getConnection();
		if (conn == null)
			return false;
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean ret = false;
		
		try {
			ps = conn.prepareStatement(model.getTestQuery());
			rs = ps.executeQuery();
			
			if (rs != null)
				rs.close();
			
			if (ps != null)
				ps.close();
			
			ret = true;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			recycle(conn);
		}
		
		return ret;
	}

}
