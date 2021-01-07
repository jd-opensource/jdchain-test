package test.com.jd.blockchain.intgr;

import com.jd.blockchain.consensus.ConsensusProviders;
import com.jd.blockchain.consensus.bftsmart.BftsmartConsensusViewSettings;
import com.jd.blockchain.crypto.AsymmetricKeypair;
import com.jd.blockchain.crypto.HashDigest;
import com.jd.blockchain.crypto.KeyGenUtils;
import com.jd.blockchain.crypto.PrivKey;
import com.jd.blockchain.crypto.PubKey;
import com.jd.blockchain.gateway.GatewayConfigProperties;
import com.jd.blockchain.ledger.BlockchainKeypair;
import com.jd.blockchain.ledger.ConsensusSettingsUpdateOperation;
import com.jd.blockchain.ledger.ContractCodeDeployOperation;
import com.jd.blockchain.ledger.ContractEventSendOperation;
import com.jd.blockchain.ledger.DataAccountKVSetOperation;
import com.jd.blockchain.ledger.DataAccountRegisterOperation;
import com.jd.blockchain.ledger.EventAccountRegisterOperation;
import com.jd.blockchain.ledger.EventPublishOperation;
import com.jd.blockchain.ledger.LedgerInitOperation;
import com.jd.blockchain.ledger.LedgerTransaction;
import com.jd.blockchain.ledger.Operation;
import com.jd.blockchain.ledger.ParticipantRegisterOperation;
import com.jd.blockchain.ledger.ParticipantStateUpdateOperation;
import com.jd.blockchain.ledger.RolesConfigureOperation;
import com.jd.blockchain.ledger.UserAuthorizeOperation;
import com.jd.blockchain.ledger.UserInfoSetOperation;
import com.jd.blockchain.ledger.UserRegisterOperation;
import com.jd.blockchain.ledger.core.LedgerQuery;
import com.jd.blockchain.sdk.BlockchainService;
import com.jd.blockchain.sdk.client.GatewayServiceFactory;
import com.jd.blockchain.storage.service.DbConnectionFactory;
import com.jd.blockchain.test.PeerServer;
import com.jd.blockchain.tools.initializer.LedgerBindingConfig;
import com.jd.blockchain.utils.Property;
import com.jd.blockchain.utils.concurrent.ThreadInvoker;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeTest;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeWeb4Nodes;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static test.com.jd.blockchain.intgr.IntegrationBase.buildLedgers;
import static test.com.jd.blockchain.intgr.IntegrationBase.peerNodeStart;
import static test.com.jd.blockchain.intgr.IntegrationBase.validKeyPair;
import static test.com.jd.blockchain.intgr.IntegrationBase.validKvWrite;

public class IntegrationTestBftsmartLC {

	private static final boolean isRegisterUser = true;

	private static final String DB_TYPE_MEM = "mem";
	public static final String DB_TYPE_ROCKSDB = "rocksdb";

	public static final String BFTSMART_PROVIDER = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";

	@Test
	public void test4Memory() throws InterruptedException {
		Configurator.setLevel("bftsmart", Level.DEBUG);
		Configurator.setLevel("com.jd.blockchain", Level.DEBUG);
		test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
	}

	@Test
	public void test4Rocksdb() throws InterruptedException {
		Configurator.setLevel("bftsmart", Level.DEBUG);
		Configurator.setLevel("com.jd.blockchain", Level.DEBUG);
		test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_ROCKSDB, LedgerInitConsensusConfig.rocksdbConnectionStrings);
	}

	public void test(String[] providers, String dbType, String[] dbConnections) throws InterruptedException {

		final ExecutorService sendReqExecutors = Executors.newFixedThreadPool(20);

		// 内存账本初始化
		HashDigest ledgerHash = initLedger(dbConnections);

		// 启动Peer节点
		PeerServer[] peerNodes = peerNodeStart(ledgerHash, dbType);

		// 休眠20秒，保证Peer节点启动成功
		Thread.sleep(20000);

		String encodedBase58Pwd = KeyGenUtils.encodePasswordAsBase58(LedgerInitializeTest.PASSWORD);

		GatewayConfigProperties.KeyPairConfig gwkey0 = new GatewayConfigProperties.KeyPairConfig();
		gwkey0.setPubKeyValue(IntegrationBase.PUB_KEYS[0]);
		gwkey0.setPrivKeyValue(IntegrationBase.PRIV_KEYS[0]);
		gwkey0.setPrivKeyPassword(encodedBase58Pwd);

		GatewayTestRunner gateway = new GatewayTestRunner("127.0.0.1", 11000, gwkey0, providers, null,
				peerNodes[2].getServiceAddress());

		ThreadInvoker.AsyncCallback<Object> gwStarting = gateway.start();

		gwStarting.waitReturn();

		GatewayServiceFactory gwsrvFact = GatewayServiceFactory.connect(gateway.getServiceAddress());

		PrivKey privkey0 = KeyGenUtils.decodePrivKeyWithRawPassword(IntegrationBase.PRIV_KEYS[0],
				IntegrationBase.PASSWORD);

		PubKey pubKey0 = KeyGenUtils.decodePubKey(IntegrationBase.PUB_KEYS[0]);

		AsymmetricKeypair adminKey = new AsymmetricKeypair(pubKey0, privkey0);

		BlockchainService blockchainService = gwsrvFact.getBlockchainService();

		int size = 2;
		CountDownLatch countDownLatch = new CountDownLatch(size);
		if (isRegisterUser) {
			for (int i = 0; i < size; i++) {
				sendReqExecutors.execute(() -> {

					System.out.printf(" sdk execute time = %s threadId = %s \r\n", System.currentTimeMillis(),
							Thread.currentThread().getId());
					IntegrationBase.KeyPairResponse userResponse = IntegrationBase.testSDK_RegisterUser(adminKey,
							ledgerHash, blockchainService);

//                    validKeyPair(userResponse, ledgerRepository, IntegrationBase.KeyPairType.USER);
					countDownLatch.countDown();
				});
			}
		}

		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread.sleep(30000);
		peerNodes[0].stop();

		try {
			System.out.println("----------------- Init Completed -----------------");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private HashDigest initLedger(String[] dbConnections) {
		LedgerInitializeWeb4Nodes ledgerInit = new LedgerInitializeWeb4Nodes();
		HashDigest ledgerHash = ledgerInit.testInitWith4Nodes(LedgerInitConsensusConfig.bftsmartConfig, dbConnections);
		System.out.printf("LedgerHash = %s \r\n", ledgerHash.toBase58());
		return ledgerHash;
	}
}
