package test.com.jd.binary.protocal.perf;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

@DataContract(code = 100)
public interface Bill {
	
	@DataField(order = 0, primitiveType = PrimitiveType.INT32)
	int getId();
	
	@DataField(order = 1, primitiveType = PrimitiveType.TEXT)
	String getTitle();
	
	@DataField(order = 2, primitiveType = PrimitiveType.INT64)
	long getValue();
	
	@DataField(order = 3, primitiveType = PrimitiveType.BYTES)
	byte[] getContent();
	
	@DataField(order = 4, refContract = true)
	People getOwner();
	
}
