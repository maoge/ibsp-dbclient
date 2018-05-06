package ibsp.dbclient.pool;

import java.sql.Connection;

import javax.sql.DataSource;

public interface ConnectionPool extends AutoCloseable {

	/**
	 * 从连接池中获取数据库连接
	 * @return
	 */
	public Connection getConnection();
	
	/**
	 * 暴漏出DataSource,便于和iBatis集成
	 * @return
	 */
	public DataSource getDataSource();
	
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
