package test.com.jd.blockchain.intgr;

import com.jd.blockchain.consensus.ConsensusProvider;
import com.jd.blockchain.consensus.ConsensusProviders;
import com.jd.blockchain.consensus.ConsensusViewSettings;
import com.jd.blockchain.crypto.AsymmetricKeypair;
import com.jd.blockchain.crypto.HashDigest;
import com.jd.blockchain.crypto.KeyGenUtils;
import com.jd.blockchain.crypto.PrivKey;
import com.jd.blockchain.crypto.PubKey;
import com.jd.blockchain.gateway.GatewayConfigProperties.KeyPairConfig;
import com.jd.blockchain.ledger.BlockchainKeyGenerator;
import com.jd.blockchain.ledger.BlockchainKeypair;
import com.jd.blockchain.ledger.LedgerInitProperties;
import com.jd.blockchain.ledger.LedgerTransaction;
import com.jd.blockchain.ledger.Operation;
import com.jd.blockchain.ledger.TransactionRequest;
import com.jd.blockchain.ledger.UserRegisterOperation;
import com.jd.blockchain.ledger.core.DefaultOperationHandleRegisteration;
import com.jd.blockchain.ledger.core.LedgerManage;
import com.jd.blockchain.ledger.core.LedgerQuery;
import com.jd.blockchain.ledger.core.LedgerRepository;
import com.jd.blockchain.ledger.core.OperationHandleRegisteration;
import com.jd.blockchain.ledger.core.TransactionBatchProcessor;
import com.jd.blockchain.sdk.service.PeerBlockchainServiceFactory;
import com.jd.blockchain.sdk.service.SimpleConsensusClientManager;
import com.jd.blockchain.service.TransactionBatchResultHandle;
import com.jd.blockchain.storage.service.impl.composite.CompositeConnectionFactory;
import com.jd.blockchain.test.PeerServer;
import com.jd.blockchain.tools.initializer.DBConnectionConfig;
import com.jd.blockchain.tools.initializer.LedgerBindingConfig;
import com.jd.blockchain.tools.initializer.Prompter;
import com.jd.blockchain.transaction.TxBuilder;
import com.jd.blockchain.transaction.TxContentBlob;
import com.jd.blockchain.transaction.TxRequestBuilder;
import com.jd.httpservice.utils.web.WebResponse;

import org.springframework.core.io.ClassPathResource;
import test.com.jd.blockchain.intgr.IntegratedContext.Node;
import test.com.jd.blockchain.intgr.perf.LedgerInitializeWebTest;
import test.com.jd.blockchain.intgr.perf.Utils;
import utils.concurrent.ThreadInvoker.AsyncCallback;
import utils.net.NetworkAddress;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.jd.blockchain.ledger.TransactionState.LEDGER_ERROR;
import static com.jd.blockchain.transaction.TxBuilder.computeTxContentHash;

public class TransactionsReplay {
	public static final String PASSWORD = "abc";

	public static void main_(String[] args) {

		// init ledgers of all nodes ;
		IntegratedContext context = initLedgers();

		Node node0 = context.getNode(0);
		Node node1 = context.getNode(1);
		Node node2 = context.getNode(2);
		Node node3 = context.getNode(3);

		NetworkAddress peerSrvAddr0 = new NetworkAddress("127.0.0.1", 10200);
		PeerServer peer0 = new PeerServer(peerSrvAddr0, node0.getBindingConfig(), node0.getStorageDB(),
				node0.getLedgerManager());

		NetworkAddress peerSrvAddr1 = new NetworkAddress("127.0.0.1", 10210);
		PeerServer peer1 = new PeerServer(peerSrvAddr1, node1.getBindingConfig(), node1.getStorageDB(),
				node1.getLedgerManager());

		NetworkAddress peerSrvAddr2 = new NetworkAddress("127.0.0.1", 10220);
		PeerServer peer2 = new PeerServer(peerSrvAddr2, node2.getBindingConfig(), node2.getStorageDB(),
				node2.getLedgerManager());

		NetworkAddress peerSrvAddr3 = new NetworkAddress("127.0.0.1", 10230);
		PeerServer peer3 = new PeerServer(peerSrvAddr3, node3.getBindingConfig(), node3.getStorageDB(),
				node3.getLedgerManager());

		AsyncCallback<Object> peerStarting0 = peer0.start();
		AsyncCallback<Object> peerStarting1 = peer1.start();
		AsyncCallback<Object> peerStarting2 = peer2.start();
		AsyncCallback<Object> peerStarting3 = peer3.start();

		peerStarting0.waitReturn();
		peerStarting1.waitReturn();
		peerStarting2.waitReturn();
		peerStarting3.waitReturn();

		String encodedBase58Pwd = KeyGenUtils.encodePasswordAsBase58(LedgerInitializeWebTest.PASSWORD);

		KeyPairConfig gwkey0 = new KeyPairConfig();
		gwkey0.setPubKeyValue(LedgerInitializeWebTest.PUB_KEYS[0]);
		gwkey0.setPrivKeyValue(LedgerInitializeWebTest.PRIV_KEYS[0]);
		gwkey0.setPrivKeyPassword(encodedBase58Pwd);

		PrivKey privKey = KeyGenUtils.decodePrivKeyWithRawPassword(LedgerInitializeWebTest.PRIV_KEYS[0], PASSWORD);
		PubKey pubKey = KeyGenUtils.decodePubKey(LedgerInitializeWebTest.PUB_KEYS[0]);

		GatewayTestRunner gateway0 = new GatewayTestRunner("127.0.0.1", 10300, gwkey0,
				LedgerInitConsensusConfig.bftsmartProvider, null, peerSrvAddr0);

		AsyncCallback<Object> gwStarting0 = gateway0.start();

		gwStarting0.waitReturn();

		LedgerManage ledgerManage0 = node3.getLedgerManager();

		LedgerManage ledgerManage3 = node3.getLedgerManager();

		HashDigest ledgerHash = ledgerManage3.getLedgerHashs()[0];

		LedgerRepository ledgerRepository0 = (LedgerRepository) ledgerManage0.register(ledgerHash,
				node0.getStorageDB().connect("memory://local/0").getStorageService(), node0.getBindingConfig().getLedger(ledgerHash).getDataStructure());
		LedgerRepository ledgerRepository3 = (LedgerRepository) ledgerManage3.register(ledgerHash,
				node0.getStorageDB().connect("memory://local/3").getStorageService(), node0.getBindingConfig().getLedger(ledgerHash).getDataStructure());

		addNewBlocksForNode0(node0, pubKey, privKey);

		// 验证Managecontroller中的交易重放方法
		checkLedgerDiff(ledgerRepository3, node3.getPartiKeyPair(), "127.0.0.1", "10200");

		System.out.println("ledger0 latest block height = " + ledgerRepository0.retrieveLatestBlockHeight());
		System.out.println("ledger0 latest block hash = " + ledgerRepository0.retrieveLatestBlockHash());

		System.out.println("ledger3 latest block height = " + ledgerRepository3.retrieveLatestBlockHeight());
		System.out.println("ledger3 latest block hash = " + ledgerRepository3.retrieveLatestBlockHash());
	}

	private static void addNewBlocksForNode0(Node node0, PubKey pubKey, PrivKey privKey) {
		TransactionBatchResultHandle handle = null;
		OperationHandleRegisteration opReg = new DefaultOperationHandleRegisteration();
		LedgerManage ledgerManage0 = node0.getLedgerManager();

		HashDigest ledgerHash0 = ledgerManage0.getLedgerHashs()[0];
		long startTs = System.currentTimeMillis();
		LedgerRepository ledgerRepository0 = (LedgerRepository) ledgerManage0.register(ledgerHash0,
				node0.getStorageDB().connect("memory://local/0").getStorageService(), node0.getBindingConfig().getLedger(ledgerHash0).getDataStructure());

		for (int height = 1; height < 20; height++) {
			TransactionBatchProcessor txbatchProcessor = new TransactionBatchProcessor(ledgerRepository0, opReg);
			for (int i = 0; i < 5; i++) {
				TxBuilder txbuilder = new TxBuilder(ledgerHash0,
						ledgerRepository0.getAdminSettings().getSettings().getCryptoSetting().getHashAlgorithm());
				TxContentBlob txContentBlob = new TxContentBlob(ledgerHash0);
				txContentBlob.setTime(startTs++);
				BlockchainKeypair userKey = BlockchainKeyGenerator.getInstance().generate();
				UserRegisterOperation userRegisterOperation = txbuilder.users().register(userKey.getIdentity());
				txContentBlob.addOperation(userRegisterOperation);
				HashDigest contentHash = computeTxContentHash(
						ledgerRepository0.getAdminSettings().getSettings().getCryptoSetting().getHashAlgorithm(),
						txContentBlob);
				TxRequestBuilder txRequestBuilder = new TxRequestBuilder(contentHash, txContentBlob);
				txRequestBuilder.signAsEndpoint(new AsymmetricKeypair(pubKey, privKey));
				txRequestBuilder.signAsNode(new AsymmetricKeypair(pubKey, privKey));
				txbatchProcessor.schedule(txRequestBuilder.buildRequest());
			}

			handle = txbatchProcessor.prepare();
			handle.commit();
		}
	}

	private static WebResponse checkLedgerDiff(LedgerRepository ledgerRepository, AsymmetricKeypair localKeyPair,
			String remoteManageHost, String remoteManagePort) {

//		List<String> providers = new ArrayList<String>();

		long localLatestBlockHeight = ledgerRepository.getLatestBlockHeight();

		HashDigest localLatestBlockHash = ledgerRepository.getLatestBlockHash();

		HashDigest remoteBlockHash;

		long remoteLatestBlockHeight = -1; // 激活新节点时，远端管理节点最新区块高度

		HashDigest ledgerHash = ledgerRepository.getHash();

		TransactionBatchResultHandle handle = null;

		OperationHandleRegisteration opReg = new DefaultOperationHandleRegisteration();

		try {
//			providers.add(LedgerInitConsensusConfig.bftsmartProvider[0]);
			SimpleConsensusClientManager clientManager = new SimpleConsensusClientManager();
			PeerBlockchainServiceFactory blockchainServiceFactory = PeerBlockchainServiceFactory.connect(localKeyPair,
					new NetworkAddress(remoteManageHost, Integer.parseInt(remoteManagePort)),
					EmptySessionCredentialProvider.INSTANCE, clientManager);

			remoteLatestBlockHeight = blockchainServiceFactory.getBlockchainService().getLedger(ledgerHash)
					.getLatestBlockHeight();

			if ((localLatestBlockHeight <= remoteLatestBlockHeight)) {

				// 检查本节点与拉取节点相同高度的区块，哈希是否一致,不一致说明其中一个节点的数据库被污染了
				remoteBlockHash = blockchainServiceFactory.getBlockchainService()
						.getBlock(ledgerHash, localLatestBlockHeight).getHash();

				if (!(localLatestBlockHash.toBase58().equals(remoteBlockHash.toBase58()))) {
					throw new IllegalStateException(
							"[ManagementController] checkLedgerDiff, ledger database is inconsistent, please check ledger database!");
				}

				// 本节点与拉取节点高度一致，不需要进行交易重放
				if (localLatestBlockHeight == remoteLatestBlockHeight) {
					return WebResponse.createSuccessResult(null);
				}
			} else {
				throw new IllegalStateException(
						"[ManagementController] checkLedgerDiff, local latest block height > remote node latest block height!");
			}

			// 对差异进行交易重放
			for (int height = (int) localLatestBlockHeight + 1; height <= remoteLatestBlockHeight; height++) {

				TransactionBatchProcessor txbatchProcessor = new TransactionBatchProcessor(ledgerRepository, opReg);
				// transactions replay
				try {
					HashDigest pullBlockHash = blockchainServiceFactory.getBlockchainService()
							.getBlock(ledgerHash, height).getHash();
					int preTotalCount = (int) blockchainServiceFactory.getBlockchainService()
							.getTransactionCount(ledgerHash, height - 1);
					int curTotalCount = (int) blockchainServiceFactory.getBlockchainService()
							.getTransactionCount(ledgerHash, height);
					// 获取区块内的增量交易
					int addition_count = curTotalCount - preTotalCount;

					LedgerTransaction[] transactions = blockchainServiceFactory.getBlockchainService()
							.getTransactions(ledgerHash, height, preTotalCount, addition_count);

					for (LedgerTransaction ledgerTransaction : transactions) {

						TxContentBlob txContentBlob = new TxContentBlob(ledgerHash);

						txContentBlob.setTime(ledgerTransaction.getRequest().getTransactionContent().getTimestamp());

						// convert operation, from json to object
						for (Operation operation : ledgerTransaction.getRequest().getTransactionContent()
								.getOperations()) {
							txContentBlob.addOperation(operation);
						}

						TxRequestBuilder txRequestBuilder = new TxRequestBuilder(
								ledgerTransaction.getRequest().getTransactionHash(), txContentBlob);
						txRequestBuilder.addNodeSignature(ledgerTransaction.getRequest().getNodeSignatures());
						txRequestBuilder.addEndpointSignature(ledgerTransaction.getRequest().getEndpointSignatures());
						TransactionRequest transactionRequest = txRequestBuilder.buildRequest();

						txbatchProcessor.schedule(transactionRequest);
					}

					handle = txbatchProcessor.prepare();

					if (!(handle.getBlock().getHash().toBase58().equals(pullBlockHash.toBase58()))) {
						throw new IllegalStateException(
								"[ManagementController] checkLedgerDiff, transactions replay, block hash result is inconsistent!");
					}

					handle.commit();

				} catch (Exception e) {
					handle.cancel(LEDGER_ERROR);
					throw new IllegalStateException(
							"[ManagementController] checkLedgerDiff, transactions replay failed!", e);
				}
			}
		} catch (Exception e) {
			return WebResponse.createFailureResult(-1, "[ManagementController] checkLedgerDiff error!" + e);
		}

		return WebResponse.createSuccessResult(null);
	}

	public static ConsensusProvider getConsensusProvider(String providerName) {
		return ConsensusProviders.getProvider(providerName);
	}

	private static LedgerInitProperties loadInitSetting_integration() {
		ClassPathResource ledgerInitSettingResource = new ClassPathResource("ledger_init_test_web2.init");
		try (InputStream in = ledgerInitSettingResource.getInputStream()) {
			LedgerInitProperties setting = LedgerInitProperties.resolve(in);
			return setting;
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	private static IntegratedContext initLedgers() {
		Prompter consolePrompter = new PresetAnswerPrompter("N"); // new ConsolePrompter();
		LedgerInitProperties initSetting = loadInitSetting_integration();
		Properties props = LedgerInitializeWebTest
				.loadConsensusSetting(LedgerInitConsensusConfig.bftsmartConfig.getConfigPath());
		ConsensusProvider csProvider = getConsensusProvider(LedgerInitConsensusConfig.bftsmartConfig.getProvider());
		ConsensusViewSettings csProps = csProvider.getSettingsFactory().getConsensusSettingsBuilder()
				.createSettings(props, Utils.loadParticipantNodes());

		// 启动服务器；
		NetworkAddress initAddr0 = initSetting.getConsensusParticipant(0).getInitializerAddress();
		LedgerInitializeWebTest.NodeWebContext nodeCtx0 = new LedgerInitializeWebTest.NodeWebContext(0, initAddr0);

		NetworkAddress initAddr1 = initSetting.getConsensusParticipant(1).getInitializerAddress();
		LedgerInitializeWebTest.NodeWebContext nodeCtx1 = new LedgerInitializeWebTest.NodeWebContext(1, initAddr1);

		NetworkAddress initAddr2 = initSetting.getConsensusParticipant(2).getInitializerAddress();
		LedgerInitializeWebTest.NodeWebContext nodeCtx2 = new LedgerInitializeWebTest.NodeWebContext(2, initAddr2);

		NetworkAddress initAddr3 = initSetting.getConsensusParticipant(3).getInitializerAddress();
		LedgerInitializeWebTest.NodeWebContext nodeCtx3 = new LedgerInitializeWebTest.NodeWebContext(3, initAddr3);
		PrivKey privkey0 = KeyGenUtils.decodePrivKeyWithRawPassword(LedgerInitializeWebTest.PRIV_KEYS[0],
				LedgerInitializeWebTest.PASSWORD);
		PrivKey privkey1 = KeyGenUtils.decodePrivKeyWithRawPassword(LedgerInitializeWebTest.PRIV_KEYS[1],
				LedgerInitializeWebTest.PASSWORD);
		PrivKey privkey2 = KeyGenUtils.decodePrivKeyWithRawPassword(LedgerInitializeWebTest.PRIV_KEYS[2],
				LedgerInitializeWebTest.PASSWORD);
		PrivKey privkey3 = KeyGenUtils.decodePrivKeyWithRawPassword(LedgerInitializeWebTest.PRIV_KEYS[3],
				LedgerInitializeWebTest.PASSWORD);

		String encodedPassword = KeyGenUtils.encodePasswordAsBase58(LedgerInitializeWebTest.PASSWORD);

		CountDownLatch quitLatch = new CountDownLatch(4);

		TestDbFactory dbFactory0 = new TestDbFactory(new CompositeConnectionFactory());
		DBConnectionConfig testDb0 = new DBConnectionConfig();
		testDb0.setConnectionUri("memory://local/0");
		LedgerBindingConfig bindingConfig0 = new LedgerBindingConfig();
		AsyncCallback<HashDigest> callback0 = nodeCtx0.startInitCommand(privkey0, encodedPassword, initSetting, csProps,
				csProvider, testDb0, consolePrompter, bindingConfig0, quitLatch, dbFactory0);

		TestDbFactory dbFactory1 = new TestDbFactory(new CompositeConnectionFactory());
		DBConnectionConfig testDb1 = new DBConnectionConfig();
		testDb1.setConnectionUri("memory://local/1");
		LedgerBindingConfig bindingConfig1 = new LedgerBindingConfig();
		AsyncCallback<HashDigest> callback1 = nodeCtx1.startInitCommand(privkey1, encodedPassword, initSetting, csProps,
				csProvider, testDb1, consolePrompter, bindingConfig1, quitLatch, dbFactory1);

		TestDbFactory dbFactory2 = new TestDbFactory(new CompositeConnectionFactory());
		DBConnectionConfig testDb2 = new DBConnectionConfig();
		testDb2.setConnectionUri("memory://local/2");
		LedgerBindingConfig bindingConfig2 = new LedgerBindingConfig();
		AsyncCallback<HashDigest> callback2 = nodeCtx2.startInitCommand(privkey2, encodedPassword, initSetting, csProps,
				csProvider, testDb2, consolePrompter, bindingConfig2, quitLatch, dbFactory2);

		TestDbFactory dbFactory3 = new TestDbFactory(new CompositeConnectionFactory());
		DBConnectionConfig testDb3 = new DBConnectionConfig();
		testDb3.setConnectionUri("memory://local/3");
		LedgerBindingConfig bindingConfig3 = new LedgerBindingConfig();
		AsyncCallback<HashDigest> callback3 = nodeCtx3.startInitCommand(privkey3, encodedPassword, initSetting, csProps,
				csProvider, testDb3, consolePrompter, bindingConfig3, quitLatch, dbFactory3);

		HashDigest ledgerHash0 = callback0.waitReturn();
		HashDigest ledgerHash1 = callback1.waitReturn();
		HashDigest ledgerHash2 = callback2.waitReturn();
		HashDigest ledgerHash3 = callback3.waitReturn();

		LedgerQuery ledger0 = nodeCtx0.registerLedger(ledgerHash0, initSetting.getLedgerDataStructure());
		LedgerQuery ledger1 = nodeCtx1.registerLedger(ledgerHash1, initSetting.getLedgerDataStructure());
		LedgerQuery ledger2 = nodeCtx2.registerLedger(ledgerHash2, initSetting.getLedgerDataStructure());
		LedgerQuery ledger3 = nodeCtx3.registerLedger(ledgerHash3, initSetting.getLedgerDataStructure());

		System.out.println("ledger hash 1 = " + ledger0.getLatestBlock().getHash().toBase58());
		System.out.println("ledger hash 2 = " + ledger1.getLatestBlock().getHash().toBase58());
		System.out.println("ledger hash 3 = " + ledger2.getLatestBlock().getHash().toBase58());
		System.out.println("ledger hash 4 = " + ledger3.getLatestBlock().getHash().toBase58());

		IntegratedContext context = new IntegratedContext();

		context.setLedgerHash(ledgerHash0);

		Node node0 = new Node(0);
		node0.setConsensusSettings(csProps);
		node0.setLedgerManager(nodeCtx0.getLedgerManager());
		node0.setStorageDB(nodeCtx0.getStorageDB());
		node0.setPartiKeyPair(new AsymmetricKeypair(initSetting.getConsensusParticipant(0).getPubKey(), privkey0));
		node0.setBindingConfig(bindingConfig0);
		node0.setConnectionConfig(testDb0);
		context.addNode(node0);

		Node node1 = new Node(1);
		node1.setConsensusSettings(csProps);
		node1.setLedgerManager(nodeCtx1.getLedgerManager());
		node1.setStorageDB(nodeCtx1.getStorageDB());
		node1.setPartiKeyPair(new AsymmetricKeypair(initSetting.getConsensusParticipant(1).getPubKey(), privkey1));
		node1.setBindingConfig(bindingConfig1);
		node1.setConnectionConfig(testDb1);
		context.addNode(node1);

		Node node2 = new Node(2);
		node2.setConsensusSettings(csProps);
		node2.setLedgerManager(nodeCtx2.getLedgerManager());
		node2.setStorageDB(nodeCtx2.getStorageDB());
		node2.setPartiKeyPair(new AsymmetricKeypair(initSetting.getConsensusParticipant(2).getPubKey(), privkey2));
		node2.setBindingConfig(bindingConfig2);
		node2.setConnectionConfig(testDb2);
		context.addNode(node2);

		Node node3 = new Node(3);
		node3.setConsensusSettings(csProps);
		node3.setLedgerManager(nodeCtx3.getLedgerManager());
		node3.setStorageDB(nodeCtx3.getStorageDB());
		node3.setPartiKeyPair(new AsymmetricKeypair(initSetting.getConsensusParticipant(3).getPubKey(), privkey3));
		node3.setBindingConfig(bindingConfig3);
		node3.setConnectionConfig(testDb3);
		context.addNode(node3);

		nodeCtx0.closeServer();
		nodeCtx1.closeServer();
		nodeCtx2.closeServer();
		nodeCtx3.closeServer();

		return context;
	}

}
