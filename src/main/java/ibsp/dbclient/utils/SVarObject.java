package ibsp.dbclient.utils;

public class SVarObject {

	private String val;

	public SVarObject() {
		val = "";
	}

	public SVarObject(String s) {
		this.val = s;
	}

	public String getVal() {
		return val;
	}

	public void setVal(String val) {
		this.val = val;
	}

	public void clear() {
		this.val = "";
	}

}