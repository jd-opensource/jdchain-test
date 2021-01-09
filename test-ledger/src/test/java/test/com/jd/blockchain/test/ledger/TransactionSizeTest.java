package test.com.jd.blockchain.test.ledger;

import org.junit.Test;

import com.jd.binaryproto.BinaryProtocol;
import com.jd.blockchain.crypto.AsymmetricKeypair;
import com.jd.blockchain.crypto.Crypto;
import com.jd.blockchain.crypto.HashDigest;
import com.jd.blockchain.crypto.HashFunction;
import com.jd.blockchain.crypto.service.classic.ClassicAlgorithm;
import com.jd.blockchain.ledger.TransactionRequest;
import com.jd.blockchain.ledger.TransactionRequestBuilder;
import com.jd.blockchain.transaction.TxBuilder;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.codec.Base58Utils;
import com.jd.blockchain.utils.security.RandomUtils;

public class TransactionSizeTest {

	@Test
	public void test() {
		HashFunction HASH_FUNC =  Crypto.getHashFunction(ClassicAlgorithm.SHA256);
		
		String key = Base58Utils.encode(RandomUtils.generateRandomBytes(96));
		byte[] value = RandomUtils.generateRandomBytes(4000);

		final HashDigest ledgerHash =HASH_FUNC.hash(RandomUtils.generateRandomBytes(32));
		final Bytes dataAccountAddress = new Bytes(RandomUtils.generateRandomBytes(32));
		
		TxBuilder builder = new TxBuilder(ledgerHash, ClassicAlgorithm.SHA256);
		builder.dataAccount(dataAccountAddress).setBytes(key, value, -1);
		
		AsymmetricKeypair kp = Crypto.getSignatureFunction(ClassicAlgorithm.ED25519).generateKeypair();
		AsymmetricKeypair kp2 = Crypto.getSignatureFunction(ClassicAlgorithm.ED25519).generateKeypair();
		
		TransactionRequestBuilder txReqBuilder = builder.prepareRequest();
		txReqBuilder.signAsEndpoint(kp);
		txReqBuilder.signAsEndpoint(kp2);
		
		TransactionRequest req = txReqBuilder.buildRequest();
		byte[] reqBytes = BinaryProtocol.encode(req, TransactionRequest.class);
		
		System.out.println("Bytes of Transaction request: " + reqBytes.length + ";("+reqBytes.length/1024f+" kb)");
	}

}
