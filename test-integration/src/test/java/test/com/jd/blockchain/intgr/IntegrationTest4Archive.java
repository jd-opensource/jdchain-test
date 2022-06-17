package test.com.jd.blockchain.intgr;

import com.jd.blockchain.crypto.AsymmetricKeypair;
import com.jd.blockchain.crypto.HashDigest;
import com.jd.blockchain.crypto.KeyGenUtils;
import com.jd.blockchain.crypto.PrivKey;
import com.jd.blockchain.crypto.PubKey;
import com.jd.blockchain.gateway.GatewayConfigProperties;
import com.jd.blockchain.ledger.BlockchainKeypair;
import com.jd.blockchain.ledger.core.LedgerQuery;
import com.jd.blockchain.sdk.BlockchainService;
import com.jd.blockchain.sdk.client.GatewayServiceFactory;
import com.jd.blockchain.storage.service.DbConnectionFactory;
import com.jd.blockchain.test.PeerServer;
import com.jd.blockchain.tools.initializer.LedgerBindingConfig;
import com.jd.httpservice.agent.ServiceConnectionManager;
import com.jd.httpservice.agent.ServiceEndpoint;
import com.jd.httpservice.converters.JsonResponseConverter;
import com.jd.httpservice.utils.web.WebResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeTest;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeWeb4Nodes;
import utils.concurrent.ThreadInvoker;
import utils.net.SSLSecurity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static test.com.jd.blockchain.intgr.IntegrationBase.buildLedgers;
import static test.com.jd.blockchain.intgr.IntegrationBase.peerNodeStart;
import static test.com.jd.blockchain.intgr.IntegrationBase.validKeyPair;
import static test.com.jd.blockchain.intgr.IntegrationBase.validKvWrite;

public class IntegrationTest4Archive {

	private static final boolean isRegisterUser = true;

	private static final boolean isRegisterDataAccount = true;

	private static final boolean isWriteKv = true;

	public static final String DB_TYPE_ROCKSDB = "rocksdb";

	public static final String BFTSMART_PROVIDER = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";

	@Test
	public void test4Rocksdb() {
		Configurator.setLevel("bftsmart", Level.DEBUG);
		Configurator.setLevel("com.jd.blockchain", Level.DEBUG);
		test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_ROCKSDB, LedgerInitConsensusConfig.rocksdbConnectionStrings);
	}

	public void test(String[] providers, String dbType, String[] dbConnections) {

		final ExecutorService sendReqExecutors = Executors.newFixedThreadPool(20);

		// 内存账本初始化
		HashDigest ledgerHash = initLedger(dbConnections);

		// 启动Peer节点
		PeerServer[] peerNodes = peerNodeStart(ledgerHash, dbType);

		try {
			// 休眠20秒，保证Peer节点启动成功
			Thread.sleep(20000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		DbConnectionFactory dbConnectionFactory0 = peerNodes[0].getDBConnectionFactory();
		DbConnectionFactory dbConnectionFactory1 = peerNodes[1].getDBConnectionFactory();
		DbConnectionFactory dbConnectionFactory2 = peerNodes[2].getDBConnectionFactory();
		DbConnectionFactory dbConnectionFactory3 = peerNodes[3].getDBConnectionFactory();

		String encodedBase58Pwd = KeyGenUtils.encodePasswordAsBase58(LedgerInitializeTest.PASSWORD);

		GatewayConfigProperties.KeyPairConfig gwkey0 = new GatewayConfigProperties.KeyPairConfig();
		gwkey0.setPubKeyValue(IntegrationBase.PUB_KEYS[0]);
		gwkey0.setPrivKeyValue(IntegrationBase.PRIV_KEYS[0]);
		gwkey0.setPrivKeyPassword(encodedBase58Pwd);

		GatewayTestRunner gateway = new GatewayTestRunner("127.0.0.1", 11000, gwkey0, providers, null,
				peerNodes[0].getServiceAddress());

		ThreadInvoker.AsyncCallback<Object> gwStarting = gateway.start();

		gwStarting.waitReturn();

		// 执行测试用例之前，校验每个节点的一致性；
		LedgerQuery[] ledgers = buildLedgers(
				new LedgerBindingConfig[] { peerNodes[0].getLedgerBindingConfig(),
						peerNodes[1].getLedgerBindingConfig(), peerNodes[2].getLedgerBindingConfig(),
						peerNodes[3].getLedgerBindingConfig(), },
				new DbConnectionFactory[] { dbConnectionFactory0, dbConnectionFactory1, dbConnectionFactory2,
						dbConnectionFactory3 });

		IntegrationBase.testConsistencyAmongNodes(ledgers);

		LedgerQuery ledgerRepository = ledgers[0];

		try {
			// 休眠20秒，保证Peer节点启动成功
			Thread.sleep(20000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		GatewayServiceFactory gwsrvFact = GatewayServiceFactory.connect(gateway.getServiceAddress());

		PrivKey privkey0 = KeyGenUtils.decodePrivKeyWithRawPassword(IntegrationBase.PRIV_KEYS[0],
				IntegrationBase.PASSWORD);

		PubKey pubKey0 = KeyGenUtils.decodePubKey(IntegrationBase.PUB_KEYS[0]);

		AsymmetricKeypair adminKey = new AsymmetricKeypair(pubKey0, privkey0);

		BlockchainService blockchainService = gwsrvFact.getBlockchainService();

		int size = 15;
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

		if (isRegisterDataAccount) {
			IntegrationBase.KeyPairResponse dataAccountResponse = IntegrationBase.testSDK_RegisterDataAccount(adminKey,
					ledgerHash, blockchainService);

			validKeyPair(dataAccountResponse, ledgerRepository, IntegrationBase.KeyPairType.DATAACCOUNT);

			if (isWriteKv) {

				for (int m = 0; m < 13; m++) {
					BlockchainKeypair da = dataAccountResponse.keyPair;
					IntegrationBase.KvResponse kvResponse = IntegrationBase.testSDK_InsertData(adminKey, ledgerHash,
							blockchainService, da.getAddress());
					validKvWrite(kvResponse, ledgerRepository, blockchainService);
				}
			}
		}

		try {
			System.out.println("----------------- Init Completed -----------------");

			// exception block height parameter check start
//		    kvDataArchive(ledgerHash.toBase58(), LedgerInitConsensusConfig.rocksdbConnectionStrings[4], "123456", 0, 0);
//
//			kvDataArchive(ledgerHash.toBase58(), LedgerInitConsensusConfig.rocksdbConnectionStrings[4], "123456", -1, 0);
//
//			kvDataArchive(ledgerHash.toBase58(), LedgerInitConsensusConfig.rocksdbConnectionStrings[4], "123456", 1, 0);
//
//			kvDataArchive(ledgerHash.toBase58(), LedgerInitConsensusConfig.rocksdbConnectionStrings[4], "123456", 1, 50);

			// exception block height parameter check end

			kvDataArchive(ledgerHash.toBase58(), "127.0.0.1", 12000, 3, 10);
			kvDataArchive(ledgerHash.toBase58(), "127.0.0.1", 12000, 12, 13);
			kvDataRecovery(ledgerHash.toBase58(), "127.0.0.1", 12000, 3, 10);
			kvDataIterate(ledgerHash.toBase58(), "127.0.0.1", 12000);
			kvDataRecovery(ledgerHash.toBase58(), "127.0.0.1", 12000, 12, 13);
			kvDataIterate(ledgerHash.toBase58(), "127.0.0.1", 12000);

			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

		IntegrationBase.testConsistencyAmongNodes(ledgers);
	}

	private HashDigest initLedger(String[] dbConnections) {
		LedgerInitializeWeb4Nodes ledgerInit = new LedgerInitializeWeb4Nodes();
		HashDigest ledgerHash = ledgerInit.testInitWith4Nodes(LedgerInitConsensusConfig.bftsmartConfig, dbConnections);
		System.out.printf("LedgerHash = %s \r\n", ledgerHash.toBase58());
		return ledgerHash;
	}

	private void kvDataArchive(String ledgerHash, String ip, int port, long startHeight, long endHeight) {
		try {
			String url = "http://" + ip + ":" + String.valueOf(port) + "/management/delegate/kvdataarchive";
			HttpPost httpPost = new HttpPost(url);
			List<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();
			params.add(new BasicNameValuePair("ledgerHash", ledgerHash));
			params.add(new BasicNameValuePair("fromHeight", String.valueOf(startHeight)));
			params.add(new BasicNameValuePair("toHeight", String.valueOf(endHeight)));
			httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
			ServiceEndpoint endpoint = new ServiceEndpoint(ip, port, false);

			endpoint.setSslSecurity(new SSLSecurity());

			HttpResponse response = ServiceConnectionManager.buildHttpClient(endpoint).execute(httpPost);
			WebResponse webResponse = (WebResponse) new JsonResponseConverter(WebResponse.class).getResponse(null, response.getEntity().getContent(), null);

			if (webResponse.isSuccess()) {
				System.out.println("kvDataArchive succ!");
			} else {
				System.out.println("kvDataArchive fail! " + webResponse.getError().getErrorMessage());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e.getMessage());
		}
	}

	private void kvDataRecovery(String ledgerHash, String ip, int port, long startHeight, long endHeight) {
		try {
			String url = "http://" + ip + ":" + String.valueOf(port) + "/management/delegate/kvdatarecovery";
			HttpPost httpPost = new HttpPost(url);
			List<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();
			params.add(new BasicNameValuePair("ledgerHash", ledgerHash));
			params.add(new BasicNameValuePair("fromHeight", String.valueOf(startHeight)));
			params.add(new BasicNameValuePair("toHeight", String.valueOf(endHeight)));
			httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
			ServiceEndpoint endpoint = new ServiceEndpoint(ip, port, false);

			endpoint.setSslSecurity(new SSLSecurity());

			HttpResponse response = ServiceConnectionManager.buildHttpClient(endpoint).execute(httpPost);
			WebResponse webResponse = (WebResponse) new JsonResponseConverter(WebResponse.class).getResponse(null, response.getEntity().getContent(), null);

			if (webResponse.isSuccess()) {
				System.out.println("kvDataRecovery succ!");
			} else {
				System.out.println("kvDataRecovery fail! " + webResponse.getError().getErrorMessage());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e.getMessage());
		}
	}

	private void kvDataIterate(String ledgerHash, String ip, int port) {
		try {
			String url = "http://" + ip + ":" + String.valueOf(port) + "/management/delegate/kvdataiterate";
			HttpPost httpPost = new HttpPost(url);
			List<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();
			params.add(new BasicNameValuePair("ledgerHash", ledgerHash));
			httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
			ServiceEndpoint endpoint = new ServiceEndpoint(ip, port, false);

			endpoint.setSslSecurity(new SSLSecurity());

			HttpResponse response = ServiceConnectionManager.buildHttpClient(endpoint).execute(httpPost);
			WebResponse webResponse = (WebResponse) new JsonResponseConverter(WebResponse.class).getResponse(null, response.getEntity().getContent(), null);

			if (webResponse.isSuccess()) {
				System.out.println("kvDataIterate succ!");
			} else {
				System.out.println("kvDataIterate fail! " + webResponse.getError().getErrorMessage());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e.getMessage());
		}
	}
}
