package ibsp.dbclient.utils;

public class DbProperties {
	
	private static DbProperties instance = null;
	private static Object mtx = null;
	
	private String dbSourceIds;
	private String metaSvrRootUrl;
	
	static {
		mtx = new Object();
	}
	
	public static DbProperties get() {
		if (instance != null)
			return instance;
		
		synchronized(mtx) {
			if (instance == null) {
				instance = new DbProperties();
			}
			
			return instance;
		}
	}
	
	public DbProperties() {
		this.dbSourceIds    = PropertiesUtils.getInstance(CONSTS.INIT_PROP_FILE).get("dbsource.ids");
		this.metaSvrRootUrl = PropertiesUtils.getInstance(CONSTS.INIT_PROP_FILE).get("metasvr.rooturl");
	}

	public String getDbSourceIds() {
		return dbSourceIds;
	}

	public void setDbSourceIds(String dbSourceIds) {
		this.dbSourceIds = dbSourceIds;
	}

	public String getMetaSvrRootUrl() {
		return metaSvrRootUrl;
	}

	public void setMetaSvrRootUrl(String metaSvrRootUrl) {
		this.metaSvrRootUrl = metaSvrRootUrl;
	}
	
}
