package test.perf.com.jd.blockchain.ledger;

import java.io.IOException;
import java.util.Random;

import com.jd.blockchain.crypto.Crypto;
import com.jd.blockchain.crypto.CryptoProvider;
import com.jd.blockchain.crypto.service.classic.ClassicAlgorithm;
import com.jd.blockchain.crypto.service.classic.ClassicCryptoService;
import com.jd.blockchain.crypto.service.sm.SMCryptoService;
import com.jd.blockchain.ledger.CryptoSetting;
import com.jd.blockchain.ledger.core.CryptoConfig;
import com.jd.blockchain.ledger.proof.MerkleHashTrie;
import com.jd.blockchain.storage.service.utils.MemoryKVStorage;
import com.jd.blockchain.utils.Bytes;

public class MerkleTreePerformanceTester {

	public static final String LEDGER_KEY_PREFIX = "LDG://";

	private static final String[] SUPPORTED_PROVIDERS = { ClassicCryptoService.class.getName(),
			SMCryptoService.class.getName() };

	public static void main_(String[] args) {
		try {
			int round = 10;
			if (args.length > 0) {
				for (int i = 0; i < args.length; i++) {
					if (args[i].startsWith("-Dround=")) {
						String strRound = args[i].substring("-Dround=".length());
						try {
							round = Integer.parseInt(strRound.trim());
						} catch (NumberFormatException e) {
							System.out.println("Error round argument!--" + e.getMessage());
							e.printStackTrace();
						}
					}
				}
			}
			
			testPerformace1(round, 50, 2000);

			System.out.println("----------- End Test -----------");
			
			System.out.println("Press any key to quit...");
			System.in.read();
			
		} catch (Exception e) {
			System.out.println("Error occurred!!! --" + e.getMessage());
			e.printStackTrace();
		}
	}

	private static void testPerformace1(int round, int batch, int count) {
		System.out.printf("------------- Performance test: HashSortingMerkleTree --------------\r\n", batch, count);

		CryptoSetting setting = createDefaultCryptoSetting();
		Bytes prefix = Bytes.fromString(LEDGER_KEY_PREFIX);

		Random rand = new Random();
		byte[] value = new byte[128];
		rand.nextBytes(value);

		System.out.println("All ready, press any key to start...");
		try {
			System.in.read();
		} catch (IOException e) {
		}
		for (int i = 0; i < round; i++) {
			System.out.printf("------------- Test Round [%s] --------------\r\n", i);
			testPerformance(setting, prefix, value, batch, count);
			System.gc();
		}
	}

	private static void testPerformance(CryptoSetting setting, Bytes prefix, byte[] value, int batch, int count) {
		
		long startTs = System.currentTimeMillis();

		MemoryKVStorage storage = new MemoryKVStorage();
		MerkleHashTrie merkleTree = new MerkleHashTrie(setting, prefix, storage);
		String key;
		for (int r = 0; r < batch; r++) {
			for (int i = 0; i < count; i++) {
				key = "KEY-" + r + "-" + i;
				merkleTree.setData(key, 0, value);
			}
			merkleTree.commit();
		}

		long elapsedTs = System.currentTimeMillis() - startTs;

		long totalCount = count * batch;
		double tps = batch * 1000.0D / elapsedTs;
		double kps = batch * count * 1000.0D / elapsedTs;
		System.out.printf("--[Performance]:: TotalKeys=%s; Batch=%s; Count=%s; Times=%sms; TPS=%.2f; KPS=%.2f\r\n\r\n",
				totalCount, batch, count, elapsedTs, tps, kps);
	}

	public static CryptoSetting createDefaultCryptoSetting() {

		CryptoProvider[] supportedProviders = getContextProviders();

		CryptoConfig cryptoSetting = new CryptoConfig();
		cryptoSetting.setSupportedProviders(supportedProviders);
		cryptoSetting.setAutoVerifyHash(true);
		cryptoSetting.setHashAlgorithm(ClassicAlgorithm.SHA256);
		return cryptoSetting;
	}

	public static CryptoProvider[] getContextProviders() {
		CryptoProvider[] supportedProviders = new CryptoProvider[SUPPORTED_PROVIDERS.length];
		for (int i = 0; i < SUPPORTED_PROVIDERS.length; i++) {
			supportedProviders[i] = Crypto.getProvider(SUPPORTED_PROVIDERS[i]);
		}

		return supportedProviders;
	}
}
