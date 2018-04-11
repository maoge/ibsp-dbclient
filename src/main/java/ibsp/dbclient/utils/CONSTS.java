package ibsp.dbclient.utils;

public class CONSTS {

	public static final String DBPOOL_PROP_FILE        = "conf/dbpool";
	public static final String INIT_PROP_FILE          = "conf/init";
	
	public static final String META_SERVICE            = "metasvr";
	public static final String TIDB_SERVICE            = "tidbsvr";
	
	public static final String FUN_URL_TEST            = "test";
	public static final String FUN_GET_ADDRESS         = "getTidbInfoByService";
	public static final String FUN_PUT_STATISTIC_INFO  = "putClientStatisticInfo";
	
	public static final String HTTP_PROTOCAL           = "http";
	public static final String HTTP_METHOD_GET         = "GET";
	public static final String HTTP_METHOD_POST        = "POST";
	
	public static final String JSON_HEADER_RET_CODE    = "RET_CODE";
	public static final String JSON_HEADER_RET_INFO    = "RET_INFO";
	public static final String JSON_HEADER_REMOTE_IP   = "REMOTE_IP";
	
	public static final int REVOKE_OK                  = 0;
	public static final int REVOKE_NOK                 = -1;
	
	public static final String JDBC_HEADER             = "jdbc:mysql://";
	
	
	//event listen and dispatch
	public static final int FIX_HEAD_LEN    = 10;
	public static final int FIX_PREHEAD_LEN = 6;
	public static final byte[] PRE_HEAD     = {'$','H','E','A','D',':'};
	
	public static final int BASE_PORT = 9500;
	public static final int BATCH_FIND_CNT = 1000;
	public static final int GET_IP_RETRY = 5;
	public static final int GET_IP_RETRY_INTERVAL = 500;
	public static final int REPORT_INTERVAL        = 10000; // 定时上报间隔
	public static final int EVENT_DISPACH_INTERVAL = 10;    // 事件派发空闲休眠间隔
	public static final int RECONNECT_INTERVAL     = 1000;  // 重连间隔
	
	public static final String PARAM_CLIENT_TYPE = "CLIENT_TYPE";
	public static final String PARAM_LSNR_ADDR = "LSNR_ADDR";
	public static final String PARAM_CLIENT_INFO = "CLIENT_INFO";
	
	public static final String TYPE_DB_CLIENT = "DB_CLIENT";

	public static final String EV_CODE = "EVENT_CODE";
	public static final String EV_SERV_ID = "SERV_ID";
	public static final String EV_JSON_STR = "JSON_STR";
}
