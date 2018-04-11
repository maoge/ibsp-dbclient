package ibsp.dbclient.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import ibsp.dbclient.DbSource;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.exception.DBException.DBERRINFO;
import ibsp.dbclient.utils.CONSTS;
import ibsp.dbclient.utils.HttpUtils;
import ibsp.dbclient.utils.SVarObject;

/***
 * 从metaserver获得配置信息, 以单例模式对外提供使用 
 */
public class MetasvrConfigFactory {
	
	private static Logger logger = LoggerFactory.getLogger(MetasvrConfigFactory.class);
	private static final ReentrantLock monitor = new ReentrantLock();
	private static MetasvrConfigFactory instance = null;
	
	private MetasvrUrlConfig metasvrUrl; //metaserver address
	private Map<String, String> dbAddress;

	public static MetasvrConfigFactory getInstance() {
		return instance;
	}
	
	public static MetasvrConfigFactory getInstance(String metasvrUrl) throws DBException {
		monitor.lock();
		try {
			if (instance == null) {
				instance = new MetasvrConfigFactory(metasvrUrl);
			}
		} finally {
			monitor.unlock();
		}
		return instance;
	}
	
	private MetasvrConfigFactory(String url) throws DBException {
		this.metasvrUrl = new MetasvrUrlConfig(url);
		this.dbAddress = new HashMap<String, String>();
		
		//process tidb addresses
		String serviceID = DbConfig.get().getServiceID();
		if (serviceID==null || serviceID.isEmpty()) {
			throw new DBException("serviceID is empty!", new Throwable(), DBERRINFO.e1);
		}
		String initUrl = String.format("%s/%s/%s?%s", metasvrUrl.getNextUrl(), 
				CONSTS.TIDB_SERVICE, CONSTS.FUN_GET_ADDRESS, "SERV_ID="+DbConfig.get().getServiceID());
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.getData(initUrl, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			if (jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE) == CONSTS.REVOKE_OK) {
				JSONArray array = jsonObj.getJSONArray(CONSTS.JSON_HEADER_RET_INFO);
				for (int i=0; i<array.size(); i++) {
					JSONObject tidbServer = array.getJSONObject(i);
					this.dbAddress.put(tidbServer.getString("ID"), tidbServer.getString("ADDRESS"));
				}
			}
		}
		if (dbAddress==null || dbAddress.size()==0) {
			throw new DBException("TIDB address url is empty!", new Throwable(), DBERRINFO.e1);
		}
	}
	
	public String getMetasvrUrl() {
		return metasvrUrl.getNextUrl();
	}
	
	public void putBrokenUrl(String url) {
		metasvrUrl.putBrokenUrl(url);
	}
	
	public void doUrlCheck() {
		metasvrUrl.doUrlCheck();
	}
	
	public Map<String, String> getDbAddress() {
		return this.dbAddress;
	}
	
	public void addDbAddress(String servID, String instID, String address) {
		if (servID.equals(DbConfig.get().getServiceID())) {
			this.dbAddress.put(instID, address);
			try {
				DbSource.get().addPool(instID, address);
			} catch (Exception e) {
				logger.error("Add DB connection pool failed...", e);
			}
		}
	}
	
	public void removeDbAddress(String servID, String instID) {
		if (servID.equals(DbConfig.get().getServiceID())) {
			this.dbAddress.remove(instID);
			try {
				DbSource.get().removePool(instID);
			} catch (Exception e) {
				logger.error("Remove DB connection pool failed...", e);
			}
		}
	}

	public synchronized void close() {
		metasvrUrl.close();
		if (instance!=null) {
			instance = null;
		}
	}
}
