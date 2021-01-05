package test.com.jd.binary.protocal.perf;

import java.io.Serializable;

public class BillData implements Bill, Serializable{
	
	private static final long serialVersionUID = -2799801173660652307L;
	
	private int id;
	private String title;
	private long value;
	private People owner;

	private byte[] content;
	
	
	public BillData() {
	}
	
	
	public BillData(int id, String title, long value, byte[] content, People owner) {
		this.id = id;
		this.title = title;
		this.value = value;
		this.content = content;
		this.owner = owner;
	}
	

	@Override
	public int getId() {
		return id;
	}

	@Override
	public String getTitle() {
		return title;
	}

	@Override
	public long getValue() {
		return value;
	}
	
	@Override
	public byte[] getContent() {
		return content;
	}

	@Override
	public People getOwner() {
		return owner;
	}

}
