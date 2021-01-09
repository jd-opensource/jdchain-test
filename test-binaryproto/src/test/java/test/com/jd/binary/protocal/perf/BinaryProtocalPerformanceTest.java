package test.com.jd.binary.protocal.perf;

import java.util.concurrent.Callable;

import org.junit.Test;

import com.google.protobuf.ByteString;
import com.jd.binaryproto.BinaryProtocol;
import com.jd.blockchain.utils.ConsoleUtils;
import com.jd.blockchain.utils.security.RandomUtils;
import com.jd.blockchain.utils.serialize.binary.BinarySerializeUtils;

import test.com.jd.binary.protocal.perf.protobuf.BillProto;

public class BinaryProtocalPerformanceTest {

	@Test
	public void test() throws Exception {
		Location homeAddress = new LocationData(10010, "ABCDEFGHIJKLMNOPQRSTUVWXYZ-University", new long[] { 10011, 58, 32, 19992 });
		People owner = new PeopleData(1,
				"John-[1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ]", 100,
				homeAddress);
		byte[] content = RandomUtils.generateRandomBytes(64);
		Bill bill = new BillData(100, "Bill of Order[1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ]",
				100, content, owner);

		ConsoleUtils.info("\r\n\r\n================== 测试1：对原始对象的序列化... ========================");
		Callable<byte[]> jdkSerTask1 = createSerializeTask_JDK(bill);
		Callable<byte[]> bpSerTask1 = createSerialzeTask_BinaryProtol(bill);
		Callable<byte[]> ppSerTask1 = createSerializeTask_Protobuf(bill);
		byte[] jdkOutputBytes = testSerialize("JDK Serialize-ORIG", jdkSerTask1, true, 100000);
		byte[] bpOutputBytes = testSerialize("BinaryProtol Serialize-ORIG", bpSerTask1, true, 100000);
		byte[] ppOutputBytes = testSerialize("Protobuf Serialize-ORIG", ppSerTask1, true, 100000);

		ConsoleUtils.info("\r\n\r\n================== 测试2：对字节数据反序列化为对象... ========================");
		Callable<Bill> jdkDesTask = createDeserialzeTask_JDK(jdkOutputBytes);
		Callable<Bill> bpDesTask = createDeserialzeTask_BinaryProtol(bpOutputBytes);
		Callable<Bill> ppDesTask = createDeserialzeTask_Protobuf(ppOutputBytes);
		Bill jdkDesObject = testDeserialize("JDK Deserialize", jdkDesTask, true, 100000);
		Bill bpDesObject = testDeserialize("BinaryProtol Deserialize", bpDesTask, true, 100000);
		Bill ppDesObject = testDeserialize("Protobuf Deserialize", ppDesTask, true, 100000);

		ConsoleUtils.info("\r\n\r\n================== 测试3：对反序列化对象的再次序列化... ========================");
		Callable<byte[]> jdkSerTask2 = createSerializeTask_JDK(jdkDesObject);
		Callable<byte[]> bpSerTask2 = createSerialzeTask_BinaryProtol(bpDesObject);
		Callable<byte[]> ppSerTask2 = createSerializeTask_Protobuf(ppDesObject);
		byte[] jdkOutputBytes2 = testSerialize("JDK Serialize-REP", jdkSerTask2, true, 100000);
		byte[] bpOutputBytes2 = testSerialize("BinaryProtol Serialize-REP", bpSerTask2, true, 100000);
		byte[] ppOutputBytes2 = testSerialize("Protobuf Serialize-REP", ppSerTask2, true, 100000);

	}

	private Callable<byte[]> createSerialzeTask_BinaryProtol(Bill bill) {
		return new Callable<byte[]>() {
			@Override
			public byte[] call() throws Exception {
				return BinaryProtocol.encode(bill, Bill.class);
			}
		};
	}

	private Callable<byte[]> createSerializeTask_JDK(Bill bill) {
		return new Callable<byte[]>() {
			@Override
			public byte[] call() throws Exception {
				return BinarySerializeUtils.serialize(bill);
			}
		};
	}

	private Callable<byte[]> createSerializeTask_Protobuf(Bill bill) {
		return new Callable<byte[]>() {
			@Override
			public byte[] call() throws Exception {

				BillProto.Location locationProto;
				BillProto.Location.Builder locationProtoBuilder = BillProto.Location.newBuilder();
				locationProtoBuilder.setCode(bill.getOwner().getHomeAddress().getCode())
						.setName(bill.getOwner().getHomeAddress().getName());
				long[] posistion = bill.getOwner().getHomeAddress().getPosition();
				for (long l : posistion) {
					locationProtoBuilder.addPosition(l);
				}
				locationProto = locationProtoBuilder.build();

				BillProto.People ownerProto = BillProto.People.newBuilder()//
						.setId(bill.getOwner().getId())//
						.setName(bill.getOwner().getName())//
						.setAge(bill.getOwner().getAge())//
						.setHomeAddress(locationProto)//
						.build();
				BillProto.Bill billProto = BillProto.Bill.newBuilder().setId(bill.getId()).setTitle(bill.getTitle())
						.setValue(bill.getValue()).setContent(ByteString.copyFrom(bill.getContent()))
						.setOwner(ownerProto).build();
				return billProto.toByteArray();
			}
		};
	}

	private Callable<Bill> createDeserialzeTask_BinaryProtol(byte[] billBytes) {
		return new Callable<Bill>() {
			@Override
			public Bill call() throws Exception {
				return BinaryProtocol.decode(billBytes, Bill.class);
			}
		};
	}

	private Callable<Bill> createDeserialzeTask_JDK(byte[] billBytes) {
		return new Callable<Bill>() {
			@Override
			public Bill call() throws Exception {
				return BinarySerializeUtils.deserialize(billBytes);
			}
		};
	}

	private Callable<Bill> createDeserialzeTask_Protobuf(byte[] billBytes) {
		return new Callable<Bill>() {
			@Override
			public Bill call() throws Exception {
				BillProto.Bill billProto = BillProto.Bill.parseFrom(billBytes);
				BillProto.Location homeAddress = billProto.getOwner().getHomeAddress();
				long[] posistions = new long[homeAddress.getPositionCount()];
				for (int i = 0; i < posistions.length; i++) {
					posistions[i] = homeAddress.getPosition(i);
				}
				BillData bill = new BillData(billProto.getId(), billProto.getTitle(), billProto.getValue(),
						billProto.getContent().toByteArray(),
						new PeopleData(billProto.getOwner().getId(), billProto.getOwner().getName(),
								billProto.getOwner().getAge(),
								new LocationData(homeAddress.getCode(), homeAddress.getName(), posistions)));
				return bill;
			}
		};
	}

	private byte[] testSerialize(String taskName, Callable<byte[]> task, boolean predo, int testCount)
			throws Exception {
		return doTest(taskName, task, predo, testCount, new Printer<byte[]>() {
			@Override
			public void print(double tps, long timeMillis, byte[] result) {
				ConsoleUtils.info("【%30s】Performance::[Size= %3s bytes][TPS = %12.2f ][TimeMillis= %4s ms]", taskName,
						result.length, tps, timeMillis);
			}
		});
	}

	private Bill testDeserialize(String taskName, Callable<Bill> task, boolean predo, int testCount) throws Exception {
		return doTest(taskName, task, predo, testCount, new Printer<Bill>() {
			@Override
			public void print(double tps, long timeMillis, Bill result) {
				ConsoleUtils.info("【%30s】 Performance::[TPS = %12.2f ][TimeMillis= %4s ms]", taskName, tps, timeMillis);
			}
		});
	}

	private <T> T doTest(String taskName, Callable<T> task, boolean predo, int testCount, Printer<T> printer)
			throws Exception {
		if (predo) {
			for (int i = 0; i < 10000; i++) {
				task.call();
			}
		}

		long startTs = System.currentTimeMillis();
//		ConsoleUtils.info("\r\n-------- START 【%s】 --------", taskName);

		T result = null;
		for (int i = 0; i < testCount; i++) {
			result = task.call();
		}
		long endTs = System.currentTimeMillis();
		double tps = testCount * 1000.0D / (endTs - startTs);
		printer.print(tps, endTs - startTs, result);
//		ConsoleUtils.info("-------- END 【%s】 --------", taskName);

		return result;
	}

	private static interface Printer<T> {

		void print(double tps, long timeMillis, T result);

	}

}
