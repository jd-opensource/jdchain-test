package test.com.jd.binary.protocal.perf;

import java.io.Serializable;

public class LocationData implements Location, Serializable{
	
	private static final long serialVersionUID = -313962262375900207L;
	
	private long code;
	private String name;
	private long[] position;

	public LocationData() {
	}
	
	public LocationData(long code, String name, long[] position) {
		this.code = code;
		this.name = name;
		this.position = position;
	}

	@Override
	public long getCode() {
		return code;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public long[] getPosition() {
		return position;
	}

}
