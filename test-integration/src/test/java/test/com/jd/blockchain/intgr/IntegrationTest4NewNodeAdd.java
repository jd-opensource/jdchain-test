package test.com.jd.blockchain.intgr;

import com.jd.blockchain.consensus.ConsensusProviders;
import com.jd.blockchain.consensus.bftsmart.BftsmartConsensusSettings;
import com.jd.blockchain.crypto.*;
import com.jd.blockchain.gateway.GatewayConfigProperties;
import com.jd.blockchain.ledger.*;
import com.jd.blockchain.ledger.core.LedgerQuery;
import com.jd.blockchain.sdk.BlockchainService;
import com.jd.blockchain.sdk.client.GatewayServiceFactory;
import com.jd.blockchain.sdk.mananger.ParticipantManager;
import com.jd.blockchain.storage.service.DbConnectionFactory;
import com.jd.blockchain.test.PeerServer;
import com.jd.blockchain.tools.initializer.LedgerBindingConfig;
import com.jd.blockchain.utils.concurrent.ThreadInvoker;
import com.jd.blockchain.utils.net.NetworkAddress;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeTest;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeWeb4Nodes;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static test.com.jd.blockchain.intgr.IntegrationBase.*;

public class IntegrationTest4NewNodeAdd {

    static String NEW_PUB = "3snPdw7i7PkdgqiGX7GbZuFSi1cwZn7vtjw4vifb1YoXgr9k6Kfmis";
    static String NEW_PRIV = "177gjtZu8w1phqHFVNiFhA35cfimXmP6VuqrBFhfbXBWK8s4TRwro2tnpffwP1Emwr6SMN6";

    public static final String PASSWORD = "abc";

    private static final boolean isRegisterParticipant = true;

    public static final String DB_TYPE_ROCKSDB = "rocksdb";

    public static final  String  BFTSMART_PROVIDER = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";

    private HashDigest ledgerHash;

    private static final String NEW_NODE_HOST = "127.0.0.1";
    private static final int NEW_NODE_HTTP_PORT = 12040;
    private static final int NEW_NODE_CONSENSUS_PORT = 20000;
    private static final int NEW_NODE_ID = 4;

    @Before
    public void init() throws Exception {
        for (int i = 0; i < 4; i++) {
            String oldDbUrl = LedgerInitConsensusConfig.rocksdbConnectionStrings[i];
            File oldNodeFile = new File(oldDbUrl.substring("rocksdb://".length()));
            if (oldNodeFile.exists()) {
                FileUtils.forceDelete(oldNodeFile);
            }
        }
    }

    @Test
    public void testPK() {
        String pk = "177gju9p5zrNdHJVEQnEEKF4ZjDDYmAXyfG84V5RPGVc5xFfmtwnHA7j51nyNLUFffzz5UT";
        String pwd = "DYu3G8aGTMBW1WrTw76zxQJQU4DHLw9MLyy7peG4LKkY";
        PrivKey privKey = KeyGenUtils.decodePrivKey(pk, pwd);
        System.out.println(privKey.toBase58());
    }

    @Test
    public void test4Rocksdb() throws Exception {
        try {
            PeerServer[] peerNodes = test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_ROCKSDB, LedgerInitConsensusConfig.rocksdbConnectionStrings);
            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");
            copyRocksdbToNewNode(0);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");
            startNewPeerNode(ledgerHash, DB_TYPE_ROCKSDB);
            System.out.println("---------- New Node Start Http But Without Consensus Completed ----------");
            Thread.sleep(5000);
            activePartiNode();
            System.out.println("---------- Active New Node And View Update Completed ----------");
            Thread.sleep(5000);
            registUserByNewGateway(new NetworkAddress(NEW_NODE_HOST, NEW_NODE_HTTP_PORT));
            System.out.println("---------- Access New Gateway And Regist User Completed ----------");
            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }
    }

    // 新创建一个网关，并使新加入的参与方接入该网关
    private void registUserByNewGateway(NetworkAddress peerServer) {

        String encodedBase58Pwd = KeyGenUtils.encodePasswordAsBase58(LedgerInitializeTest.PASSWORD);

        GatewayConfigProperties.KeyPairConfig gwkey1 = new GatewayConfigProperties.KeyPairConfig();
        gwkey1.setPubKeyValue(IntegrationBase.PUB_KEYS[1]);
        gwkey1.setPrivKeyValue(IntegrationBase.PRIV_KEYS[1]);
        gwkey1.setPrivKeyPassword(encodedBase58Pwd);
        GatewayTestRunner gateway = new GatewayTestRunner("127.0.0.1", 11040, gwkey1,
                peerServer, LedgerInitConsensusConfig.bftsmartProvider,null);

        ThreadInvoker.AsyncCallback<Object> gwStarting = gateway.start();

        gwStarting.waitReturn();

        GatewayServiceFactory gwsrvFact = GatewayServiceFactory.connect(gateway.getServiceAddress());

        PrivKey privkey1 = KeyGenUtils.decodePrivKeyWithRawPassword(IntegrationBase.PRIV_KEYS[1], IntegrationBase.PASSWORD);

        PubKey pubKey1 = KeyGenUtils.decodePubKey(IntegrationBase.PUB_KEYS[1]);

        AsymmetricKeypair adminKey = new AsymmetricKeypair(pubKey1, privkey1);

        BlockchainService blockchainService = gwsrvFact.getBlockchainService();

        IntegrationBase.testSDK_RegisterUser(adminKey, ledgerHash, blockchainService);

    }

    @Test
    public void testCopy() throws Exception {
        File newFile = copyRocksdbToNewNode(0);
        System.out.printf("%s -> %s \r\n", newFile.getPath(), newFile.exists());
    }

    private void activePartiNode() {
        ParticipantManager participantManager = ParticipantManager.getInstance();

        // 新参与方公私钥
        PrivKey privKey = KeyGenUtils.decodePrivKeyWithRawPassword(NEW_PRIV, PASSWORD);
        PubKey pubKey = KeyGenUtils.decodePubKey(NEW_PUB);

        System.out.println("Address = " + AddressEncoding.generateAddress(pubKey));

        BlockchainKeypair user = new BlockchainKeypair(pubKey, privKey);

        NetworkAddress newParticipant = new NetworkAddress(NEW_NODE_HOST, NEW_NODE_CONSENSUS_PORT);

        participantManager.activePeer(new NetworkAddress(NEW_NODE_HOST, NEW_NODE_HTTP_PORT, false), newParticipant, ledgerHash, user, null);

    }

    private File copyRocksdbToNewNode(int oldId) throws IOException {
        String oldDbUrl = LedgerInitConsensusConfig.rocksdbConnectionStrings[oldId];
        File  oldNodeFile = new File(oldDbUrl.substring("rocksdb://".length()));
        String newRocksdbPath = oldNodeFile.getParentFile().getPath() + File.separator + "rocksdb" + NEW_NODE_ID + ".db";
        File newFile = new File(newRocksdbPath);
        if (newFile.exists()) {
            FileUtils.forceDelete(newFile);
        }
        FileUtils.copyDirectory(oldNodeFile, newFile);
        return newFile;
    }

    public static PeerServer startNewPeerNode(HashDigest ledgerHash, String dbType) {

        NetworkAddress peerSrvAddr = new NetworkAddress(NEW_NODE_HOST, NEW_NODE_HTTP_PORT);
        LedgerBindingConfig bindingConfig = loadBindingConfig(NEW_NODE_ID, ledgerHash, dbType);
        PeerServer peer = new PeerServer(peerSrvAddr, bindingConfig);

        ThreadInvoker.AsyncCallback<Object> peerStarting = peer.start();

        peerStarting.waitReturn();

        return peer;
    }

    public PeerServer[] test(String[] providers, String dbType, String[] dbConnections) {

        final ExecutorService sendReqExecutors = Executors.newFixedThreadPool(20);

        // 内存账本初始化
        ledgerHash = initLedger(dbConnections);

        // 启动Peer节点
        PeerServer[] peerNodes = peerNodeStart(ledgerHash, dbType);

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
                peerNodes[0].getServiceAddress(), providers,null);

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



        LedgerQuery ledgerRepository = ledgers[0];

        GatewayServiceFactory gwsrvFact = GatewayServiceFactory.connect(gateway.getServiceAddress());

        PrivKey privkey0 = KeyGenUtils.decodePrivKeyWithRawPassword(IntegrationBase.PRIV_KEYS[0], IntegrationBase.PASSWORD);

        PubKey pubKey0 = KeyGenUtils.decodePubKey(IntegrationBase.PUB_KEYS[0]);

        AsymmetricKeypair adminKey = new AsymmetricKeypair(pubKey0, privkey0);

        BlockchainService blockchainService = gwsrvFact.getBlockchainService();


        long participantCount = ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipantCount();

        long userCount = ledgerRepository.getUserAccountSet(ledgerRepository.retrieveLatestBlock()).getTotal();

        System.out.printf("before add participant: participantCount = %d, userCount = %d\r\n", (int)participantCount, (int)userCount);

        KeyPairResponse participantResponse;
        if (isRegisterParticipant) {
            participantResponse = testSDK_RegisterParticipant(adminKey, ledgerHash, blockchainService);
        }

        participantCount = ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipantCount();

        userCount = ledgerRepository.getUserAccountSet(ledgerRepository.retrieveLatestBlock()).getTotal();

        System.out.printf("after add participant: participantCount = %d, userCount = %d\r\n", (int)participantCount, (int)userCount);

        BftsmartConsensusSettings consensusSettings = (BftsmartConsensusSettings) ConsensusProviders.getProvider(BFTSMART_PROVIDER).getSettingsFactory().getConsensusSettingsEncoder().decode(ledgerRepository.getAdminInfo().getSettings().getConsensusSetting().toBytes());
        System.out.printf("update participant state before ,old consensus env node num = %d\r\n", consensusSettings.getNodes().length);

        for (int i = 0; i < participantCount; i++) {
            System.out.printf("part%d state = %d\r\n",i, ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipants()[i].getParticipantNodeState().CODE);
        }

        return peerNodes;
    }
    private KeyPairResponse testSDK_RegisterParticipant(AsymmetricKeypair adminKey, HashDigest ledgerHash, BlockchainService blockchainService) {

        String PUB = "3snPdw7i7PkdgqiGX7GbZuFSi1cwZn7vtjw4vifb1YoXgr9k6Kfmis";
        String PRIV = "177gjtZu8w1phqHFVNiFhA35cfimXmP6VuqrBFhfbXBWK8s4TRwro2tnpffwP1Emwr6SMN6";
        final String PASSWORD = "abc";

        PrivKey privKey = KeyGenUtils.decodePrivKeyWithRawPassword(PRIV, PASSWORD);
        PubKey pubKey = KeyGenUtils.decodePubKey(PUB);

        BlockchainKeypair user = new BlockchainKeypair(pubKey, privKey);
        // 定义交易；
        TransactionTemplate txTpl = blockchainService.newTransaction(ledgerHash);

        txTpl.participants().register("peer4", new BlockchainIdentityData(pubKey), new NetworkAddress("127.0.0.1", 20000));

        // 签名；
        PreparedTransaction ptx = txTpl.prepare();

        HashDigest transactionHash = ptx.getHash();

        ptx.sign(adminKey);

        // 提交并等待共识返回；
        TransactionResponse txResp = ptx.commit();

        KeyPairResponse keyPairResponse = new KeyPairResponse();
        keyPairResponse.keyPair = user;
        keyPairResponse.txResp = txResp;
        keyPairResponse.txHash = transactionHash;
        return keyPairResponse;
    }

    private HashDigest initLedger(String[] dbConnections) {
        LedgerInitializeWeb4Nodes ledgerInit = new LedgerInitializeWeb4Nodes();
        HashDigest ledgerHash = ledgerInit.testInitWith4Nodes(LedgerInitConsensusConfig.bftsmartConfig, dbConnections);
        System.out.printf("LedgerHash = %s \r\n", ledgerHash.toBase58());
        return ledgerHash;
    }
}
