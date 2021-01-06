package test.com.jd.binary.protocal.perf;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

@DataContract(code = 101)
public interface People {
	
	@DataField(order = 0, primitiveType = PrimitiveType.INT32)
	int getId();
	
	@DataField(order = 1, primitiveType = PrimitiveType.TEXT)
	String getName();
	
	@DataField(order = 2, primitiveType = PrimitiveType.INT32)
	int getAge();
	
	@DataField(order = 3, refContract = true)
	Location getHomeAddress();
}
