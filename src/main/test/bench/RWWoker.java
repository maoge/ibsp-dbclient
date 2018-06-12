package bench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import ibsp.common.utils.PropertiesUtils;
import ibsp.dbclient.DbSource;
import ibsp.dbclient.pool.ConnectionPool;

public class RWWoker extends RunnerSkeleton {
	
	private static final String INS_SQL = "insert T_SMSGATEWAY_MT(submit_id,msg_id,cust_id,chan_id,task_id,"
																+ "charge_fee,charge_num,dest_addr,src_addr,msg_content,"
																+ "registered_delivery,service_id,fee_terminal_id,link_id,insert_time,"
																+ "submit_time,status,chan_status,chan_note,valid_time,"
																+ "at_time,msg_fmt,priority,msg_report,report_time,"
																+ "sp_msg_id,alternative,charged,smslabel,recv_time,"
																+ "inst_id,charge_id,create_date,hhmmss,prov_id,"
																+ "agent_acct_id,provider_id,ori_src_addr,area_code) "
																+ "values(?,?,?,?,?,"
																+ "?,?,?,?,?,"
																+ "?,?,?,?,?,"
																+ "?,?,?,?,?,"
																+ "?,?,?,?,?,"
																+ "?,?,?,?,?,"
																+ "?,?,?,?,?,"
																+ "?,?,?,?)";
	
	private static final String SEL_SQL = "select submit_id,msg_id,cust_id,chan_id,task_id,"
											+ "charge_fee,charge_num,dest_addr,src_addr,msg_content,"
											+ "registered_delivery,service_id,fee_terminal_id,link_id,insert_time,"
											+ "submit_time,status,chan_status,chan_note,valid_time,"
											+ "at_time,msg_fmt,priority,msg_report,report_time,"
											+ "sp_msg_id,alternative,charged,smslabel,recv_time,"
											+ "inst_id,charge_id,create_date,hhmmss,prov_id,"
											+ "agent_acct_id,provider_id,ori_src_addr,area_code "
											+ "from T_SMSGATEWAY_MT where msg_id = ?";
	
	private static final int CUST_ID_SEED = 100000;
	private static final int CHAN_ID_SEED = 400;
	private static final int TASK_ID_SEED = 5000000;
	private static final int CHARGE_NUM_SEED = 100;
	private static final int DEST_ADDR_SEED = 300000;
	private static final int SRC_ADDR_SEED = 130000;
	
	private static final int SHORT_MSG_LENGTH = 70;
	private static final int LONG_MSG_LENGTH = 10*70;
	
	private static String SHORT_MSG_CONTENT = null;
	private static String LONG_MSG_CONTENT = null;
	
	private int readBoundary = 0;
	private int rwSeedCnt = 0;
	private String msgId = "";
	private Random rwRand = null;
	
	private int shortMsgBoundary = 0;
	private int shortLongMsgSeedCnt = 0;
	private Random slRand = null;
	
	static {
		Random rand = new Random(System.currentTimeMillis());
		byte seedBand = 'z' - 'A';
		
		byte[] shortContent = new byte[SHORT_MSG_LENGTH];
		for (int i = 0; i < SHORT_MSG_LENGTH; i++) {
			shortContent[i] = (byte) (rand.nextInt(seedBand) + 'A');
		}
		SHORT_MSG_CONTENT = new String(shortContent);
		
		byte[] longContent = new byte[LONG_MSG_LENGTH];
		for (int i = 0; i < LONG_MSG_LENGTH; i++) {
			longContent[i] = (byte) (rand.nextInt(seedBand) + 'A');
		}
		LONG_MSG_CONTENT = new String(longContent);
	}
	
	public RWWoker(AtomicLong normalCnt, AtomicLong errorCnt, PropertiesUtils prop) {
		super(normalCnt, errorCnt);
		initOPSeedMap(prop);
	}
	
	private void initOPSeedMap(PropertiesUtils prop) {
		String rwRatio = prop.get("read.write.ratio");
		String[] arr = rwRatio.split(":");
		int readWeight  = Integer.valueOf(arr[0]);
		int writeWeight = Integer.valueOf(arr[1]);
		readBoundary = (readWeight > 0) ? readWeight : -1;
		rwSeedCnt = readWeight + writeWeight;
		rwRand = new Random(System.currentTimeMillis());
		
		String slMsgRatio = prop.get("short.long.msg.ratio");
		arr = slMsgRatio.split(":");
		int shortMsgWeight = Integer.valueOf(arr[0]);
		int longMsgWeight = Integer.valueOf(arr[1]);
		shortMsgBoundary = shortMsgWeight;
		shortLongMsgSeedCnt = shortMsgWeight + longMsgWeight;
		slRand = new Random(System.currentTimeMillis() + 37);
	}
	
	@Override
	public boolean doWork() {
		int seed = rwRand.nextInt(rwSeedCnt);
		boolean ret = false;
		if (seed < readBoundary) {
			ret = doRead(msgId);
		} else {
			ret = doWrite();
		}
		
		return ret;
	}

	private boolean doRead(String msgId) {
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
			
			PreparedStatement ps = conn.prepareStatement(SEL_SQL);
			needRecycle = true;
			
			ResultSet rs = ps.executeQuery();
			if (rs != null) {
				ret = true;
				rs.close();
			}
			
			if (ps != null) {
				ps.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (needRecycle) {
				pool.recycle(conn);
			}
		}
		
		return ret;
	}
	
	private boolean doWrite() {
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
			
			long id = LongIDGenerator.get().nextID();
			long ts = System.currentTimeMillis();
			
			PreparedStatement ps = conn.prepareStatement(INS_SQL);
			needRecycle = true;
			
			ps.setObject(1, id);                       // submit_id   BIGINT(18) not null,
			ps.setObject(2, String.format("%d", id));  // msg_id      VARCHAR(60),
			ps.setObject(3, id % CUST_ID_SEED);        // cust_id     INT(8),
			ps.setObject(4, id % CHAN_ID_SEED);        // chan_id     INT(8),
			ps.setObject(5, id % TASK_ID_SEED);        // task_id     BIGINT(12),
			ps.setObject(6, 0.0000);                   // charge_fee  DOUBLE(12,4),
			ps.setObject(7, id % CHARGE_NUM_SEED);     // charge_num  TINYINT(2),
			
			String destAddr = String.format("185%08d", id % DEST_ADDR_SEED);
			ps.setObject(8, destAddr);                 // dest_addr   VARCHAR(32),
			
			String srcAddr = String.format("185%08d", id % SRC_ADDR_SEED);
			ps.setObject(9, srcAddr);                  // src_addr    VARCHAR(32),
			
			int slSeed = slRand.nextInt(shortLongMsgSeedCnt);
			if (slSeed < shortMsgBoundary) {
				ps.setObject(10, SHORT_MSG_CONTENT);   // msg_content VARCHAR(4000),
			} else {
				ps.setObject(10, LONG_MSG_CONTENT);    // msg_content VARCHAR(4000),
			}
			
			ps.setObject(11, id % 100);                // registered_delivery SMALLINT(2) default 1,
			ps.setObject(12, "service_id");            // service_id          VARCHAR(30),
			ps.setObject(13, "fee_terminal_id");       // fee_terminal_id     VARCHAR(32),
			ps.setObject(14, "link_id");               // link_id             VARCHAR(50),
			ps.setObject(15, new Date(ts));            // insert_time         TIMESTAMP(3) not null,
			ps.setObject(16, null);                    // submit_time         TIMESTAMP(3),
			ps.setObject(17, 1);                       // status              TINYINT(1) default 0,
			ps.setObject(18, "chan_stat");             // chan_status         VARCHAR(10),
			ps.setObject(19, "chan_note");             // chan_note           VARCHAR(100),
			ps.setObject(20, "valid_time");            // valid_time          VARCHAR(17),
			ps.setObject(21, "at_time");               // at_time             VARCHAR(17),
			ps.setObject(22, id % 100);                // msg_fmt             TINYINT(2) default 15,
			ps.setObject(23, 3);                       // priority            TINYINT(1),
			ps.setObject(24, "");                      // msg_report          VARCHAR(20),
			ps.setObject(25, null);                    // report_time         TIMESTAMP(3),
			ps.setObject(26, "");                      // sp_msg_id           VARCHAR(1000),
			ps.setObject(27, 1);                       // alternative         TINYINT(1) default 0 not null,
			ps.setObject(28, 1);                       // charged             TINYINT(1) default 1 not null,
			ps.setObject(29, "smslabel");              // smslabel            VARCHAR(60),
			ps.setObject(30, null);                    // recv_time           TIMESTAMP(3),
			ps.setObject(31, id % 10);                 // inst_id             TINYINT(1) default 1,
			ps.setObject(32, "charge_id");             // charge_id           BIGINT(12) default 0,
			ps.setObject(33, null);                    // create_date         TIMESTAMP(3),
			ps.setObject(34, id%10000);                // hhmmss              SMALLINT(4),
			ps.setObject(35, 3500);                    // prov_id             SMALLINT(4) comment '目标号码归属地省份id',
			ps.setObject(36, 123456);                  // agent_acct_id       INT(8) comment '账本id',
			ps.setObject(37, 1000);                    // provider_id         SMALLINT(4) comment '运营商ID',
			ps.setObject(38, srcAddr);                 // ori_src_addr        VARCHAR(30) comment '原始源码号(客户提交切换通道前的码号)',
			ps.setObject(39, 3500);                    // area_code           VARCHAR(6) comment '手机归属地电话区号'
			
			ret = ps.executeUpdate() > 0;
			
			if (id % 10000 == 0) {
				msgId = String.format("%d", id);
			}
			
			if (ps != null) {
				ps.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (needRecycle) {
				pool.recycle(conn);
			}
		}
		
		return ret;
	}

}
