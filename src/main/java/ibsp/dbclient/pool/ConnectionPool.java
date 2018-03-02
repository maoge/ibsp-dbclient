package ibsp.dbclient.pool;

import java.sql.Connection;

public interface ConnectionPool extends AutoCloseable {

	/**
	 * 从连接池中获取数据库连接
	 * @return
	 */
	public Connection getConnection();
	
	/**
	 * 归还数据库连�?
	 * 
	 * @param conn 数据库连�?
	 */
	public void recycle(Connection conn);
	
	/**
	 * 获取连接池名�?
	 * 
	 * @return
	 */
	public String getName();

}
