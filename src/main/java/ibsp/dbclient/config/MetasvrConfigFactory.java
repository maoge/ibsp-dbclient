package ibsp.dbclient.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import ibsp.common.events.EventController;
import ibsp.common.events.EventSubscriber;
import ibsp.common.events.EventType;
import ibsp.common.utils.BasicOperation;
import ibsp.common.utils.CONSTS;
import ibsp.common.utils.HttpUtils;
import ibsp.common.utils.IBSPConfig;
import ibsp.common.utils.MetasvrUrlConfig;
import ibsp.common.utils.SVarObject;
import ibsp.common.utils.StringUtils;
import ibsp.dbclient.DbSource;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.exception.DBException.DBERRINFO;

/***
 * 从metaserver获得配置信息, 以单例模式对外提供使用 
 */
public class MetasvrConfigFactory implements EventSubscriber {
	
	private static Logger logger = LoggerFactory.getLogger(MetasvrConfigFactory.class);
	private static final ReentrantLock monitor = new ReentrantLock();
	private static MetasvrConfigFactory instance = null;
	
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
		MetasvrUrlConfig.init(url);
		EventController.getInstance().subscribe(CONSTS.TYPE_DB_CLIENT, this);
		
		this.dbAddress = new HashMap<String, String>();
		
		//process tidb addresses
		String serviceID = IBSPConfig.getInstance().getDbServiceID();
		if (serviceID==null || serviceID.isEmpty()) {
			throw new DBException("serviceID is empty!", new Throwable(), DBERRINFO.e1);
		}
		String initUrl = String.format("%s/%s/%s?%s", MetasvrUrlConfig.get().getNextUrl(), 
				CONSTS.TIDB_SERVICE, CONSTS.FUN_GET_ADDRESS, "SERV_ID=" + serviceID);
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
	
	public Map<String, String> getDbAddress() {
		return this.dbAddress;
	}
	
	@Override
	public void postEvent(JSONObject event) {
		int code = event.getInteger(CONSTS.EV_CODE);
		String servID = event.getString(CONSTS.EV_SERV_ID);
		String jsonStr = event.getString(CONSTS.EV_JSON_STR);
		JSONObject obj = JSONObject.parseObject(jsonStr);
		
		switch (EventType.get(code)) {
		case e71:
			addDbAddress(servID, obj.getString("INST_ID"), obj.getString("INST_ADD"));
			break;
		case e72:
			removeDbAddress(servID, obj.getString("INST_ID"));
			break;
		default:
			break;
		}
	}
	
	@Override
	public void doCompute() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doReport() {
		String lsnrAddr = EventController.getInstance().getLsnrAddr();
		if (StringUtils.isNullOrEmtpy(lsnrAddr))
			return;
		
		BasicOperation.putClientStatisticInfo("cureuprapapa", lsnrAddr, CONSTS.TYPE_DB_CLIENT);
	}
	
	private void addDbAddress(String servID, String instID, String address) {
		if (servID.equals(IBSPConfig.getInstance().getDbServiceID())) {
			this.dbAddress.put(instID, address);
			try {
				DbSource.get().addPool(instID, address);
			} catch (Exception e) {
				logger.error("Add DB connection pool failed...", e);
			}
		}
	}
	
	private void removeDbAddress(String servID, String instID) {
		if (servID.equals(IBSPConfig.getInstance().getDbServiceID())) {
			this.dbAddress.remove(instID);
			try {
				DbSource.get().removePool(instID);
			} catch (Exception e) {
				logger.error("Remove DB connection pool failed...", e);
			}
		}
	}

	public synchronized void close() {
		// unsubscribe and stop event controller
		EventController.getInstance().unsubscribe(CONSTS.TYPE_CACHE_CLIENT);
		EventController.getInstance().shutdown();
		
		MetasvrUrlConfig.get().close();
		
		if (instance!=null) {
			instance = null;
		}
	}

}
