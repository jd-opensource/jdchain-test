package test.com.jd.blockchain.intgr;

import com.jd.blockchain.consensus.ConsensusProviders;
import com.jd.blockchain.consensus.bftsmart.BftsmartConsensusSettings;
import com.jd.blockchain.crypto.AsymmetricKeypair;
import com.jd.blockchain.crypto.HashDigest;
import com.jd.blockchain.crypto.KeyGenUtils;
import com.jd.blockchain.crypto.PrivKey;
import com.jd.blockchain.crypto.PubKey;
import com.jd.blockchain.gateway.GatewayConfigProperties;
import com.jd.blockchain.kvdb.server.KVDBServer;
import com.jd.blockchain.kvdb.server.KVDBServerContext;
import com.jd.blockchain.kvdb.server.config.ServerConfig;
import com.jd.blockchain.ledger.BlockchainKeypair;
import com.jd.blockchain.ledger.core.LedgerQuery;
import com.jd.blockchain.sdk.BlockchainService;
import com.jd.blockchain.sdk.client.GatewayServiceFactory;
import com.jd.blockchain.storage.service.DbConnectionFactory;
import com.jd.blockchain.test.PeerServer;
import com.jd.blockchain.tools.initializer.LedgerBindingConfig;
import com.jd.blockchain.utils.concurrent.ThreadInvoker;
import com.jd.blockchain.utils.io.FileUtils;
import org.junit.Test;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeTest;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeWeb4Nodes;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static test.com.jd.blockchain.intgr.IntegrationBase.buildLedgers;
import static test.com.jd.blockchain.intgr.IntegrationBase.peerNodeStart;
import static test.com.jd.blockchain.intgr.IntegrationBase.validKeyPair;
import static test.com.jd.blockchain.intgr.IntegrationBase.validKvWrite;

public class IntegrationTest4KVDB {

    private static final String DB_TYPE_KVDB = "kvdb";
    public static final String BFTSMART_PROVIDER = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";

    @Test
    public void test4KVDB() throws Exception {
        ServerConfig config = new ServerConfig(this.getClass().getResource("/kvdb").getFile());
        FileUtils.deletePath(new File(config.getKvdbConfig().getDbsRootdir()), true);
        KVDBServer server = null;
        try {
            KVDBServerContext context = new KVDBServerContext(config);
            server = new KVDBServer(context);
            server.start();
            test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_KVDB, LedgerInitConsensusConfig.kvdbConnectionStrings);
        } finally {
            server.stop();
            FileUtils.deletePath(new File(config.getKvdbConfig().getDbsRootdir()), true);
        }
    }

    public void test(String[] providers, String dbType, String[] dbConnections) {


        final ExecutorService sendReqExecutors = Executors.newFixedThreadPool(20);


        // 内存账本初始化
        HashDigest ledgerHash = initLedger(dbConnections);

        // 启动Peer节点
        PeerServer[] peerNodes = peerNodeStart(ledgerHash, dbType);
        try {
            DbConnectionFactory dbConnectionFactory0 = peerNodes[0].getDBConnectionFactory();
            DbConnectionFactory dbConnectionFactory1 = peerNodes[1].getDBConnectionFactory();
            DbConnectionFactory dbConnectionFactory2 = peerNodes[2].getDBConnectionFactory();
            DbConnectionFactory dbConnectionFactory3 = peerNodes[3].getDBConnectionFactory();

            String encodedBase58Pwd = KeyGenUtils.encodePasswordAsBase58(LedgerInitializeTest.PASSWORD);

            GatewayConfigProperties.KeyPairConfig gwkey0 = new GatewayConfigProperties.KeyPairConfig();
            gwkey0.setPubKeyValue(IntegrationBase.PUB_KEYS[0]);
            gwkey0.setPrivKeyValue(IntegrationBase.PRIV_KEYS[0]);
            gwkey0.setPrivKeyPassword(encodedBase58Pwd);
            GatewayTestRunner gateway = new GatewayTestRunner("127.0.0.1", 11000, gwkey0,
                    peerNodes[0].getServiceAddress(), providers, null);

            ThreadInvoker.AsyncCallback<Object> gwStarting = gateway.start();

            gwStarting.waitReturn();

            // 执行测试用例之前，校验每个节点的一致性；
            LedgerQuery[] ledgers = buildLedgers(new LedgerBindingConfig[]{
                            peerNodes[0].getLedgerBindingConfig(),
                            peerNodes[1].getLedgerBindingConfig(),
                            peerNodes[2].getLedgerBindingConfig(),
                            peerNodes[3].getLedgerBindingConfig(),
                    },
                    new DbConnectionFactory[]{
                            dbConnectionFactory0,
                            dbConnectionFactory1,
                            dbConnectionFactory2,
                            dbConnectionFactory3});

            IntegrationBase.testConsistencyAmongNodes(ledgers);

            LedgerQuery ledgerRepository = ledgers[0];

            GatewayServiceFactory gwsrvFact = GatewayServiceFactory.connect(gateway.getServiceAddress());

            PrivKey privkey0 = KeyGenUtils.decodePrivKeyWithRawPassword(IntegrationBase.PRIV_KEYS[0], IntegrationBase.PASSWORD);

            PubKey pubKey0 = KeyGenUtils.decodePubKey(IntegrationBase.PUB_KEYS[0]);

            AsymmetricKeypair adminKey = new AsymmetricKeypair(pubKey0, privkey0);

            BlockchainService blockchainService = gwsrvFact.getBlockchainService();

            int size = 15;
            CountDownLatch countDownLatch = new CountDownLatch(size);
            for (int i = 0; i < size; i++) {
                sendReqExecutors.execute(() -> {

                    System.out.printf(" sdk execute time = %s threadId = %s \r\n", System.currentTimeMillis(), Thread.currentThread().getId());
                    IntegrationBase.KeyPairResponse userResponse = IntegrationBase.testSDK_RegisterUser(adminKey, ledgerHash, blockchainService);

                    validKeyPair(userResponse, ledgerRepository, IntegrationBase.KeyPairType.USER);
                    countDownLatch.countDown();
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            IntegrationBase.KeyPairResponse dataAccountResponse = IntegrationBase.testSDK_RegisterDataAccount(adminKey, ledgerHash, blockchainService);

            validKeyPair(dataAccountResponse, ledgerRepository, IntegrationBase.KeyPairType.DATAACCOUNT);

            for (int m = 0; m < 13; m++) {
                BlockchainKeypair da = dataAccountResponse.keyPair;
                IntegrationBase.KvResponse kvResponse = IntegrationBase.testSDK_InsertData(adminKey, ledgerHash, blockchainService, da.getAddress());
                validKvWrite(kvResponse, ledgerRepository, blockchainService);
            }

            long participantCount = ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipantCount();

            long userCount = ledgerRepository.getUserAccountSet(ledgerRepository.retrieveLatestBlock()).getTotal();

            System.out.printf("before add participant: participantCount = %d, userCount = %d\r\n", (int) participantCount, (int) userCount);

            IntegrationBase.KeyPairResponse participantResponse;
            participantResponse = IntegrationBase.testSDK_RegisterParticipant(adminKey, ledgerHash, blockchainService);

            participantCount = ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipantCount();

            userCount = ledgerRepository.getUserAccountSet(ledgerRepository.retrieveLatestBlock()).getTotal();

            System.out.printf("after add participant: participantCount = %d, userCount = %d\r\n", (int) participantCount, (int) userCount);

            BftsmartConsensusSettings consensusSettings = (BftsmartConsensusSettings) ConsensusProviders.getProvider(BFTSMART_PROVIDER).getSettingsFactory().getConsensusSettingsEncoder().decode(ledgerRepository.getAdminInfo().getSettings().getConsensusSetting().toBytes());
            System.out.printf("update participant state before ,old consensus env node num = %d\r\n", consensusSettings.getNodes().length);

            for (int i = 0; i < participantCount; i++) {
                System.out.printf("part%d state = %d\r\n", i, ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipants()[i].getParticipantNodeState().CODE);
            }

            IntegrationBase.testSDK_UpdateParticipantState(adminKey, new BlockchainKeypair(participantResponse.getKeyPair().getPubKey(), participantResponse.getKeyPair().getPrivKey()), ledgerHash, blockchainService);

            BftsmartConsensusSettings consensusSettingsNew = (BftsmartConsensusSettings) ConsensusProviders.getProvider(BFTSMART_PROVIDER).getSettingsFactory().getConsensusSettingsEncoder().decode(ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getSettings().getConsensusSetting().toBytes());

            System.out.printf("update participant state after ,new consensus env node num = %d\r\n", consensusSettingsNew.getNodes().length);
            for (int i = 0; i < participantCount; i++) {
                System.out.printf("part%d state = %d\r\n", i, ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipants()[i].getParticipantNodeState().CODE);
            }

            System.out.println("----------------- Init Completed -----------------");

            IntegrationBase.testConsistencyAmongNodes(ledgers);

        } finally {
            for (PeerServer peer : peerNodes) {
                peer.stop();
            }
        }
    }

    private HashDigest initLedger(String[] dbConnections) {
        LedgerInitializeWeb4Nodes ledgerInit = new LedgerInitializeWeb4Nodes();
        HashDigest ledgerHash = ledgerInit.testInitWith4Nodes(LedgerInitConsensusConfig.bftsmartConfig, dbConnections);
        System.out.printf("LedgerHash = %s \r\n", ledgerHash.toBase58());
        return ledgerHash;
    }
}
