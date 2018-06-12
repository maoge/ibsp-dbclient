package bench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import ibsp.dbclient.DbSource;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.pool.ConnectionPool;

public class DBPoolTest {

	public static void main(String[] args) {
		try {
			boolean create = testCreate();
			assert create : isError("create fail");
			
			boolean insert = testInsert();
			assert insert : isError("insert fail");
			
			boolean update = testUpdate();
			assert update : isError("update fail");
			
			boolean select = testSelect();
			assert select : isError("select fail");
			
			boolean delete = testDelete();
			assert delete : isError("delete fail");
			
			boolean drop = testDrop();
			assert drop : isError("drop fail");
			
			DbSource.get().close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static boolean testCreate() {
		boolean ret = false;
		boolean needRecycle = false;
		ConnectionPool pool = null;
		Connection conn = null;
		
		try {
			pool = DbSource.get().getPool();
			if (pool == null) {
				System.out.println("connection pool get null!");
				return false;
			}
			
			conn = pool.getConnection();
			if (conn == null) {
				System.out.println("connection get null!");
				return false;
			}
			needRecycle = true;
			
			String sql = "create table TEST(id int, name varchar(48))";
			PreparedStatement ps = null;
			
			ps = conn.prepareStatement(sql);
			System.out.println("create return:" + ps.executeUpdate());
			
			if (ps != null)
				ps.close();
			
			ret = true;
			
		} catch (DBException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (needRecycle) {
				pool.recycle(conn);
			}
		}
		
		return ret;
	}
	
	private static boolean testInsert() {
		boolean ret = false;
		boolean needRecycle = false;
		ConnectionPool pool = null;
		Connection conn = null;
		
		try {
			pool = DbSource.get().getPool();
			if (pool == null) {
				System.out.println("connection pool get null!");
				return false;
			}
			
			conn = pool.getConnection();
			if (conn == null) {
				System.out.println("connection get null!");
				return false;
			}
			needRecycle = true;
			
			String sql = "insert into TEST(id, name) values(?,?)";
			PreparedStatement ps = null;
			
			ps = conn.prepareStatement(sql);
			ps.setObject(1, 1);
			ps.setObject(2, "eric");
			System.out.println("insert return:" + ps.executeUpdate());
			
			if (ps != null)
				ps.close();
			
			conn.commit();
			
			ret = true;
			
		} catch (DBException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (needRecycle) {
				pool.recycle(conn);
			}
		}
		
		return ret;
	}
	
	private static boolean testSelect() {
		boolean ret = false;
		boolean needRecycle = false;
		ConnectionPool pool = null;
		Connection conn = null;
		
		try {
			pool = DbSource.get().getPool();
			if (pool == null) {
				System.out.println("connection pool get null!");
				return false;
			}
			
			conn = pool.getConnection();
			if (conn == null) {
				System.out.println("connection get null!");
				return false;
			}
			needRecycle = true;
			
			String sql = "select id, name from TEST";
			PreparedStatement ps = null;
			ResultSet rs = null;
			
			StringBuilder sb = new StringBuilder();
			
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int colnum = metaData.getColumnCount();
			
			System.out.println("----------------------");
			// print column
			for (int i = 1; i <= colnum; i++) {
				if (i > 1) sb.append("\t");
				
				String columnName = metaData.getColumnLabel(i).toUpperCase();
				sb.append(columnName);
			}
			System.out.println(sb.toString());
			sb.delete(0, sb.length());
			System.out.println("----------------------");
			
			while (rs.next()) {
				for (int i = 1; i <= colnum; i++) {
					if (i > 1) sb.append("\t");
					
					Object obj = rs.getObject(i);
					sb.append(obj != null ? obj.toString() : "");
				}
				System.out.println(sb.toString());
				sb.delete(0, sb.length());
			}
			System.out.println("----------------------");
			
			if (rs != null)
				rs.close();
			
			if (ps != null)
				ps.close();
			
			ret = true;
			
		} catch (DBException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (needRecycle) {
				pool.recycle(conn);
			}
		}
		
		return ret;
	}
	
	private static boolean testUpdate() {
		boolean ret = false;
		boolean needRecycle = false;
		ConnectionPool pool = null;
		Connection conn = null;
		
		try {
			pool = DbSource.get().getPool();
			if (pool == null) {
				System.out.println("connection pool get null!");
				return false;
			}
			
			conn = pool.getConnection();
			if (conn == null) {
				System.out.println("connection get null!");
				return false;
			}
			needRecycle = true;
			
			String sql = "update TEST set name = ? where id = ?";
			PreparedStatement ps = null;
			
			ps = conn.prepareStatement(sql);
			ps.setObject(1, "eric.robert");
			ps.setObject(2, 1);
			System.out.println("update return:" + ps.executeUpdate());
			
			if (ps != null)
				ps.close();
			
			conn.commit();
			
			ret = true;
			
		} catch (DBException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (needRecycle) {
				pool.recycle(conn);
			}
		}
		
		return ret;
	}
	
	private static boolean testDelete() {
		boolean ret = false;
		boolean needRecycle = false;
		ConnectionPool pool = null;
		Connection conn = null;
		
		try {
			pool = DbSource.get().getPool();
			if (pool == null) {
				System.out.println("connection pool get null!");
				return false;
			}
			
			conn = pool.getConnection();
			if (conn == null) {
				System.out.println("connection get null!");
				return false;
			}
			needRecycle = true;
			
			String sql = "delete from TEST where id = ?";
			PreparedStatement ps = null;
			
			ps = conn.prepareStatement(sql);
			ps.setObject(1, 1);
			System.out.println("delete return:" + ps.executeUpdate());
			
			if (ps != null)
				ps.close();
			
			conn.commit();
			
			ret = true;
			
		} catch (DBException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (needRecycle) {
				pool.recycle(conn);
			}
		}
		
		return ret;
	}
	
	private static boolean testDrop() {
		boolean ret = false;
		boolean needRecycle = false;
		ConnectionPool pool = null;
		Connection conn = null;
		
		try {
			pool = DbSource.get().getPool();
			if (pool == null) {
				System.out.println("connection pool get null!");
				return false;
			}
			
			conn = pool.getConnection();
			if (conn == null) {
				System.out.println("connection get null!");
				return false;
			}
			needRecycle = true;
			
			String sql = "drop table TEST";
			PreparedStatement ps = null;
			
			ps = conn.prepareStatement(sql);
			System.out.println("drop return:" + ps.executeUpdate());
			
			if (ps != null)
				ps.close();
			
			ret = true;
			
		} catch (DBException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (needRecycle) {
				pool.recycle(conn);
			}
		}
		
		return ret;
	}
	
	private static boolean isError(String errInfo) {
		System.out.println(errInfo);
		return false;
	}

}
