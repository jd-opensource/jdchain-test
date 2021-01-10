package test.com.jd.binary.protocal.perf;

import com.jd.binaryproto.DataContract;
import com.jd.binaryproto.DataField;
import com.jd.binaryproto.PrimitiveType;

@DataContract(code = 102)
public interface Location {
	
	@DataField(order = 1, primitiveType = PrimitiveType.INT64)
	long getCode();
	
	@DataField(order = 2, primitiveType = PrimitiveType.TEXT)
	String getName();
	
	@DataField(order = 3, primitiveType = PrimitiveType.INT64, list = true)
	long[] getPosition();
	
}
