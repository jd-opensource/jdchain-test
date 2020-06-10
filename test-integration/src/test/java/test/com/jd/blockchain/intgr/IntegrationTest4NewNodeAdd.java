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
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeWeb5Nodes;

import javax.validation.constraints.AssertTrue;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static test.com.jd.blockchain.intgr.IntegrationBase.*;
import static test.com.jd.blockchain.intgr.IntegrationTestTwoLedgers.consensusConfig;
import static test.com.jd.blockchain.intgr.LedgerInitConsensusConfig.rocksdbConnectionStrings;

public class IntegrationTest4NewNodeAdd {

    static String NEW_PUB1 = "3snPdw7i7PkdgqiGX7GbZuFSi1cwZn7vtjw4vifb1YoXgr9k6Kfmis";
    static String NEW_PRIV1 = "177gjtZu8w1phqHFVNiFhA35cfimXmP6VuqrBFhfbXBWK8s4TRwro2tnpffwP1Emwr6SMN6";

    static String NEW_PUB2 = "3snPdw7i7PnhPm1KakwRfQQME5g8aAtVnYznLqmHkdjem1ggQT3d21";
    static String NEW_PRIV2 = "177gjzRtJWBDoX1YaNhkvac2PgPfPuDu82B3JD1xwbAPdcyRPtJDwrRw6N4okms5Wvb6QQ5";

    public static final String NEW_PASSWORD = "123456";


    public static final String[] PUB_KEYS = { "3snPdw7i7PjVKiTH2VnXZu5H8QmNaSXpnk4ei533jFpuifyjS5zzH9",
            "3snPdw7i7PajLB35tEau1kmixc6ZrjLXgxwKbkv5bHhP7nT5dhD9eX",
            "3snPdw7i7PZi6TStiyc6mzjprnNhgs2atSGNS8wPYzhbKaUWGFJt7x",
            "3snPdw7i7PifPuRX7fu3jBjsb3rJRfDe9GtbDfvFJaJ4V4hHXQfhwk" };

    public static final String[] PRIV_KEYS = {
            "177gjzHTznYdPgWqZrH43W3yp37onm74wYXT4v9FukpCHBrhRysBBZh7Pzdo5AMRyQGJD7x",
            "177gju9p5zrNdHJVEQnEEKF4ZjDDYmAXyfG84V5RPGVc5xFfmtwnHA7j51nyNLUFffzz5UT",
            "177gjtwLgmSx5v1hFb46ijh7L9kdbKUpJYqdKVf9afiEmAuLgo8Rck9yu5UuUcHknWJuWaF",
            "177gk1pudweTq5zgJTh8y3ENCTwtSFsKyX7YnpuKPo7rKgCkCBXVXh5z2syaTCPEMbuWRns" };

    public static final String PASSWORD = "abc";

    public static PrivKey privkey0 = KeyGenUtils.decodePrivKeyWithRawPassword(PRIV_KEYS[0], PASSWORD);
    public static PrivKey privkey1 = KeyGenUtils.decodePrivKeyWithRawPassword(PRIV_KEYS[1], PASSWORD);
    public static PrivKey privkey2 = KeyGenUtils.decodePrivKeyWithRawPassword(PRIV_KEYS[2], PASSWORD);
    public static PrivKey privkey3 = KeyGenUtils.decodePrivKeyWithRawPassword(PRIV_KEYS[3], PASSWORD);

    public static PrivKey new_privkey1 = KeyGenUtils.decodePrivKeyWithRawPassword(NEW_PRIV1, PASSWORD);
    public static PrivKey new_privkey2 = KeyGenUtils.decodePrivKeyWithRawPassword(NEW_PRIV2, NEW_PASSWORD);


    public static PubKey pubKey0 = KeyGenUtils.decodePubKey(PUB_KEYS[0]);
    public static PubKey pubKey1 = KeyGenUtils.decodePubKey(PUB_KEYS[1]);
    public static PubKey pubKey2 = KeyGenUtils.decodePubKey(PUB_KEYS[2]);
    public static PubKey pubKey3 = KeyGenUtils.decodePubKey(PUB_KEYS[3]);

    public static PubKey new_pubKey1 = KeyGenUtils.decodePubKey(NEW_PUB1);
    public static PubKey new_pubKey2 = KeyGenUtils.decodePubKey(NEW_PUB2);

    public static String[] rocksdbConnectionStrings2 = new String[8];

    public static String[] rocksdbDirStrings2 = new String[8];

    private static final boolean isRegisterParticipant = true;

    public static final String DB_TYPE_ROCKSDB = "rocksdb";

    public static final  String  BFTSMART_PROVIDER = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";

    public static LedgerInitConsensusConfig.ConsensusConfig consensusConfig = new LedgerInitConsensusConfig.ConsensusConfig();

    private HashDigest ledgerHash;

    private HashDigest ledgerHash2;

    private static final String NEW_NODE_HOST1 = "127.0.0.1";
    private static final int NEW_NODE_HTTP_PORT1 = 12040;

    private static final String NEW_NODE_HOST2 = "127.0.0.1";
    private static final int NEW_NODE_HTTP_PORT2 = 12050;

    private static final int NEW_NODE_CONSENSUS_PORT1 = 20000;
    private static final int NEW_NODE_CONSENSUS_PORT2 = 20010;

    private static final int NEW_NODE_ID4 = 4;
    private static final int NEW_NODE_ID5 = 5;

    private NewParticipant newParticipant1;

    private NewParticipant newParticipant2;

    @Before
    public void init() throws Exception {
        for (int i = 0; i < 5; i++) {
            String oldDbUrl = rocksdbConnectionStrings[i];
            File oldNodeFile = new File(oldDbUrl.substring("rocksdb://".length()));
            if (oldNodeFile.exists()) {
                FileUtils.forceDelete(oldNodeFile);
            }
        }

        String path = LedgerInitConsensusConfig.class.getResource("/").getPath();

        String currDir = path + "rocks.db";

        // 第二个账本的rocksdb存储从标识6开始
        for (int i = 0; i < rocksdbConnectionStrings2.length; i++) {
            String dbDir = currDir + File.separator + "rocksdb" + (i+6) + ".db";
            rocksdbDirStrings2[i] = dbDir;
            rocksdbConnectionStrings2[i] = "rocksdb://" + dbDir;
        }

        newParticipant1 = new NewParticipant(4, "peer4", new_pubKey1, new_privkey1, new NetworkAddress(NEW_NODE_HOST1, NEW_NODE_HTTP_PORT1), new NetworkAddress(NEW_NODE_HOST1, NEW_NODE_CONSENSUS_PORT1));

        newParticipant2 = new NewParticipant(5, "peer5", new_pubKey2, new_privkey2, new NetworkAddress(NEW_NODE_HOST2, NEW_NODE_HTTP_PORT2), new NetworkAddress(NEW_NODE_HOST2, NEW_NODE_CONSENSUS_PORT2));
    }

    @Test
    public void testPK() {
        String pk = "177gju9p5zrNdHJVEQnEEKF4ZjDDYmAXyfG84V5RPGVc5xFfmtwnHA7j51nyNLUFffzz5UT";
        String pwd = "DYu3G8aGTMBW1WrTw76zxQJQU4DHLw9MLyy7peG4LKkY";
        PrivKey privKey = KeyGenUtils.decodePrivKey(pk, pwd);
        System.out.println(privKey.toBase58());
    }

    // 为新参与方创建一个新的网关，并验证通过新网关注册用户是否能成功
    @Test
    public void test4RocksdbAddNewNodeAndPublishTxByNewGateway() throws Exception {
        try {
            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            // 启动一个新的参与方，此时只启动HTTP服务，共识服务未开启
            startNewPeerNode(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1);
            System.out.println("---------- New Node Start Http But Without Consensus Completed ----------");

            // 激活新参与方的共识状态，更新原有共识网络的视图ID，启动新的参与方共识
            Thread.sleep(5000);
            activePartiNode(newParticipant1, ledgerHash);
            System.out.println("---------- Active New Node And View Update Completed ----------");

////            registUserByNewGateway(new NetworkAddress(NEW_NODE_HOST, NEW_NODE_HTTP_PORT));
//            System.out.println("---------- Access New Gateway And Regist User Completed ----------");
            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }
    }

    // 新参与方动态入网成功后， 通过老的网关提交交易，验证老网关的配置能否更新，交易提交能否成功
    @Test
    public void test4RocksdbAddNewNodeAndPublishTxByOldGateway() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            // 启动一个新的参与方，此时只启动HTTP服务，共识服务未开启
            startNewPeerNode(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1);
            System.out.println("---------- New Node Start Http But Without Consensus Completed ----------");

            // 激活新参与方的共识状态，更新原有共识网络的视图ID，启动新的参与方共识
            Thread.sleep(5000);
            activePartiNode(newParticipant1, ledgerHash);
            System.out.println("---------- Active New Node And View Update Completed ----------");

            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
            TransactionResponse txResp = registUserByExistGateway(blockchainService);

            assertEquals(txResp.getExecutionState(), TransactionState.SUCCESS);

            System.out.println("---------- After Add New Node, Commit Tx By Old Gateway Completed----------");

            // 再次发送交易检查网关本地的视图配置能否正确更新
            TransactionResponse txResp1 = registUserByExistGateway(blockchainService);

            assertEquals(txResp1.getExecutionState(), TransactionState.SUCCESS);

            System.out.println("---------- After Add New Node, Commit Tx By Old Gateway Completed Again----------");

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }


    }

    // 验证依次动态入网两个参与方，并验证后续交易能否正常
    @Test
    public void test4RocksdbAddTwoNewNodeAndPublishTxByOldGateway() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            // 启动一个新的参与方，此时只启动HTTP服务，共识服务未开启
            startNewPeerNode(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1);
            System.out.println("---------- New Node Start Http But Without Consensus Completed ----------");

            // 激活新参与方的共识状态，更新原有共识网络的视图ID，启动新的参与方共识
            Thread.sleep(5000);
            activePartiNode(newParticipant1, ledgerHash);
            System.out.println("---------- Active New Node And View Update Completed ----------");

            // 注册新的参与方2
            registParticipantByGateway0(blockchainService, newParticipant2, ledgerHash);

            Thread.sleep(5000);
            System.out.println("----------Regist Participant2 Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 5);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            // 启动一个新的参与方2，此时只启动HTTP服务，共识服务未开启
            startNewPeerNode(ledgerHash, DB_TYPE_ROCKSDB, newParticipant2);
            System.out.println("---------- New Node Start Http But Without Consensus Completed 2----------");

            // 激活新参与方2的共识状态，更新原有共识网络的视图ID，启动新的参与方共识
            Thread.sleep(5000);
            activePartiNode(newParticipant2, ledgerHash);
            System.out.println("---------- Active New Node And View Update Completed 2----------");


            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
            registUserByExistGateway(blockchainService);

            // 再次发送交易检查网关本地的视图配置能否正确更新
            TransactionResponse txResp = registUserByExistGateway(blockchainService);

             assertEquals(txResp.getExecutionState(), TransactionState.SUCCESS);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 初始化5个共识节点，新参与方6动态入网成功后， 通过老的网关提交交易，验证老网关的配置能否更新，交易提交能否成功
    @Test
    public void test5RocksdbAddNewNodeAndPublishTxByOldGateway() throws Exception {
        try {

            consensusConfig.provider = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";
            consensusConfig.configPath = "bftsmart-ledger2.config";

            //账本初始化
            ledgerHash = initLedger5Nodes(rocksdbConnectionStrings2);

            // 启动5个Peer节点
            PeerServer[] peerNodes = peerNodeStart5(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant2, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode5(0, 5);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            // 启动一个新的参与方，此时只启动HTTP服务，共识服务未开启
            startNewPeerNode(ledgerHash, DB_TYPE_ROCKSDB, newParticipant2);
            System.out.println("---------- New Node Start Http But Without Consensus Completed ----------");

            // 激活新参与方的共识状态，更新原有共识网络的视图ID，启动新的参与方共识
            Thread.sleep(5000);
            activePartiNode(newParticipant2, ledgerHash);
            System.out.println("---------- Active New Node And View Update Completed ----------");

            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
            registUserByExistGateway(blockchainService);

            // 再次发送交易检查网关本地的视图配置能否正确更新
            TransactionResponse txResp = registUserByExistGateway(blockchainService);

            assertEquals(txResp.getExecutionState(), TransactionState.SUCCESS);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    @Test
    public void initTwoLedgers() throws Exception {

        // 初始化第一个账本
        ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

        // 启动4个Peer节点
        PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

        // 创建连接peer0的网关
        BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes);

        // 注册新的参与方
        registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

        Thread.sleep(5000);
        System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

        // 手动复制账本
        copyRocksdbToNewNode(0, 4);
        System.out.println("---------- DataBase Copy To New Node Completed ----------");

        // 启动一个新的参与方，此时只启动HTTP服务，共识服务未开启
        startNewPeerNode(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1);
        System.out.println("---------- New Node Start Http But Without Consensus Completed ----------");

        // 激活新参与方的共识状态，更新原有共识网络的视图ID，启动新的参与方共识
        Thread.sleep(5000);
        activePartiNode(newParticipant1, ledgerHash);
        System.out.println("---------- Active New Node And View Update Completed ----------");

        Thread.sleep(5000);

        consensusConfig.provider = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";
        consensusConfig.configPath = "bftsmart-ledger2.config";

        // 开始第二个账本的初始化
        ledgerHash2 = initLedger5Nodes(rocksdbConnectionStrings2);

        // 在第二个账本上启动5个Peer节点
        PeerServer[] newLedgerPeerNodes = peerNodeStart5(ledgerHash2, DB_TYPE_ROCKSDB);

        Thread.sleep(Integer.MAX_VALUE);
    }

    private HashDigest initLedger5Nodes(String[] dbConnections) {
        LedgerInitializeWeb5Nodes ledgerInit = new LedgerInitializeWeb5Nodes();
        HashDigest ledgerHash = ledgerInit.testInitWith5Nodes(consensusConfig, dbConnections);
        System.out.printf("LedgerHash = %s \r\n", ledgerHash.toBase58());
        return ledgerHash;
    }

    public static PeerServer[] peerNodeStart4(HashDigest ledgerHash, String dbType) {
        NetworkAddress peerSrvAddr0 = new NetworkAddress("127.0.0.1", 12000);
        LedgerBindingConfig bindingConfig0 = loadBindingConfig(0, ledgerHash, dbType);
        PeerServer peer0 = new PeerServer(peerSrvAddr0, bindingConfig0);

        NetworkAddress peerSrvAddr1 = new NetworkAddress("127.0.0.1", 12010);
        LedgerBindingConfig bindingConfig1 = loadBindingConfig(1, ledgerHash, dbType);
        PeerServer peer1 = new PeerServer(peerSrvAddr1, bindingConfig1);

        NetworkAddress peerSrvAddr2 = new NetworkAddress("127.0.0.1", 12020);
        LedgerBindingConfig bindingConfig2 = loadBindingConfig(2, ledgerHash, dbType);
        PeerServer peer2 = new PeerServer(peerSrvAddr2, bindingConfig2);

        NetworkAddress peerSrvAddr3 = new NetworkAddress("127.0.0.1", 12030);
        LedgerBindingConfig bindingConfig3 = loadBindingConfig(3, ledgerHash, dbType);
        PeerServer peer3 = new PeerServer(peerSrvAddr3, bindingConfig3);

        ThreadInvoker.AsyncCallback<Object> peerStarting0 = peer0.start();
        ThreadInvoker.AsyncCallback<Object> peerStarting1 = peer1.start();
        ThreadInvoker.AsyncCallback<Object> peerStarting2 = peer2.start();
        ThreadInvoker.AsyncCallback<Object> peerStarting3 = peer3.start();

        peerStarting0.waitReturn();
        peerStarting1.waitReturn();
        peerStarting2.waitReturn();
        peerStarting3.waitReturn();

        return new PeerServer[] { peer0, peer1, peer2, peer3 };
    }

    public static PeerServer[] peerNodeStart5(HashDigest ledgerHash, String dbType) {
        NetworkAddress peerSrvAddr0 = new NetworkAddress("127.0.0.1", 11990);
        LedgerBindingConfig bindingConfig0 = loadBindingConfig(6, ledgerHash, dbType);
        PeerServer peer0 = new PeerServer(peerSrvAddr0, bindingConfig0);

        NetworkAddress peerSrvAddr1 = new NetworkAddress("127.0.0.1", 12000);
        LedgerBindingConfig bindingConfig1 = loadBindingConfig(7, ledgerHash, dbType);
        PeerServer peer1 = new PeerServer(peerSrvAddr1, bindingConfig1);

        NetworkAddress peerSrvAddr2 = new NetworkAddress("127.0.0.1", 12010);
        LedgerBindingConfig bindingConfig2 = loadBindingConfig(8, ledgerHash, dbType);
        PeerServer peer2 = new PeerServer(peerSrvAddr2, bindingConfig2);

        NetworkAddress peerSrvAddr3 = new NetworkAddress("127.0.0.1", 12020);
        LedgerBindingConfig bindingConfig3 = loadBindingConfig(9, ledgerHash, dbType);
        PeerServer peer3 = new PeerServer(peerSrvAddr3, bindingConfig3);

        NetworkAddress peerSrvAddr4 = new NetworkAddress("127.0.0.1", 12030);
        LedgerBindingConfig bindingConfig4 = loadBindingConfig(10, ledgerHash, dbType);
        PeerServer peer4 = new PeerServer(peerSrvAddr4, bindingConfig4);

        ThreadInvoker.AsyncCallback<Object> peerStarting0 = peer0.start();
        ThreadInvoker.AsyncCallback<Object> peerStarting1 = peer1.start();
        ThreadInvoker.AsyncCallback<Object> peerStarting2 = peer2.start();
        ThreadInvoker.AsyncCallback<Object> peerStarting3 = peer3.start();
        ThreadInvoker.AsyncCallback<Object> peerStarting4 = peer4.start();

        peerStarting0.waitReturn();
        peerStarting1.waitReturn();
        peerStarting2.waitReturn();
        peerStarting3.waitReturn();
        peerStarting4.waitReturn();

        return new PeerServer[] { peer0, peer1, peer2, peer3, peer4};
    }

    public AsymmetricKeypair getGw0KeyPair() {
        PrivKey privkey0 = KeyGenUtils.decodePrivKeyWithRawPassword(IntegrationBase.PRIV_KEYS[0], IntegrationBase.PASSWORD);

        PubKey pubKey0 = KeyGenUtils.decodePubKey(IntegrationBase.PUB_KEYS[0]);

        AsymmetricKeypair adminKey = new AsymmetricKeypair(pubKey0, privkey0);

        return adminKey;
    }

    public TransactionResponse registUserByExistGateway(BlockchainService blockchainService) {

        AsymmetricKeypair adminKey = getGw0KeyPair();

        // 注册用户，并验证最终写入；
        BlockchainKeypair user = BlockchainKeyGenerator.getInstance().generate();

        // 定义交易；
        TransactionTemplate txTpl = blockchainService.newTransaction(ledgerHash);
        txTpl.users().register(user.getIdentity());

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
        return txResp;

    }
    // 新创建一个网关，并使网关接入新的参与方， 参数为新参与方的HTTP服务地址
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
        File newFile = copyRocksdbToNewNode(0, 4);
        System.out.printf("%s -> %s \r\n", newFile.getPath(), newFile.exists());
    }

    private TransactionResponse activePartiNode(NewParticipant participant, HashDigest ledgerHash) {

        ParticipantManager participantManager = ParticipantManager.getInstance();

        System.out.println("Address = " + AddressEncoding.generateAddress(participant.getPubKey()));

        BlockchainKeypair user = new BlockchainKeypair(participant.getPubKey(), participant.getPrivKey());

        AsymmetricKeypair existSignerPair = new BlockchainKeypair(pubKey1, privkey1);

        return participantManager.activePeer(participant.getHttpSetting(), participant.getConsensusSetting(), ledgerHash, user, existSignerPair);

    }

    private File copyRocksdbToNewNode(int oldId, int newId) throws IOException {
        String oldDbUrl = rocksdbConnectionStrings[oldId];
        File  oldNodeFile = new File(oldDbUrl.substring("rocksdb://".length()));
        String newRocksdbPath = oldNodeFile.getParentFile().getPath() + File.separator + "rocksdb" + newId + ".db";
        File newFile = new File(newRocksdbPath);
        if (newFile.exists()) {
            FileUtils.forceDelete(newFile);
        }
        FileUtils.copyDirectory(oldNodeFile, newFile);
        return newFile;
    }

    private File copyRocksdbToNewNode5(int oldId, int newId) throws IOException {
        String oldDbUrl = rocksdbConnectionStrings2[oldId];
        File  oldNodeFile = new File(oldDbUrl.substring("rocksdb://".length()));
        String newRocksdbPath = oldNodeFile.getParentFile().getPath() + File.separator + "rocksdb" + newId + ".db";
        File newFile = new File(newRocksdbPath);
        if (newFile.exists()) {
            FileUtils.forceDelete(newFile);
        }
        FileUtils.copyDirectory(oldNodeFile, newFile);
        return newFile;
    }

    public static PeerServer startNewPeerNode(HashDigest ledgerHash, String dbType, NewParticipant newParticipant) {

        NetworkAddress peerSrvAddr = newParticipant.getHttpSetting();
        LedgerBindingConfig bindingConfig = loadBindingConfig(newParticipant.getId(), ledgerHash, dbType);
        PeerServer peer = new PeerServer(peerSrvAddr, bindingConfig);

        ThreadInvoker.AsyncCallback<Object> peerStarting = peer.start();

        peerStarting.waitReturn();

        return peer;
    }

    public BlockchainService createBlockChainService(String[] providers, PeerServer[] peerNodes) {
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

        GatewayServiceFactory gwsrvFact = GatewayServiceFactory.connect(gateway.getServiceAddress());

        BlockchainService blockchainService = gwsrvFact.getBlockchainService();

        return blockchainService;
    }

    public void registParticipantByGateway0(BlockchainService blockchainService, NewParticipant newParticipant, HashDigest ledgerHash) {

        AsymmetricKeypair adminKey = getGw0KeyPair();

        System.out.println("Address = " + AddressEncoding.generateAddress(newParticipant.getPubKey()));

        BlockchainKeypair user = new BlockchainKeypair(newParticipant.getPubKey(), newParticipant.getPrivKey());
        // 定义交易；
        TransactionTemplate txTpl = blockchainService.newTransaction(ledgerHash);

        txTpl.participants().register(newParticipant.getName(), new BlockchainIdentityData(newParticipant.getPubKey()), newParticipant.getConsensusSetting());

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
        return;
    }

    private HashDigest initLedger4Nodes(String[] dbConnections) {
        LedgerInitializeWeb4Nodes ledgerInit = new LedgerInitializeWeb4Nodes();
        HashDigest ledgerHash = ledgerInit.testInitWith4Nodes(LedgerInitConsensusConfig.bftsmartConfig, dbConnections);
        System.out.printf("LedgerHash = %s \r\n", ledgerHash.toBase58());
        return ledgerHash;
    }

//    private HashDigest initLedger5Nodes(String[] dbConnections) {
//        LedgerInitializeWeb5Nodes ledgerInit = new LedgerInitializeWeb5Nodes();
//        HashDigest ledgerHash = ledgerInit.testInitWith5Nodes(LedgerInitConsensusConfig.bftsmartConfig, dbConnections);
//        System.out.printf("LedgerHash = %s \r\n", ledgerHash.toBase58());
//        return ledgerHash;
//    }

    static class NewParticipant {
        int id;
        String name;
        PubKey pubKey;
        PrivKey privKey;
        NetworkAddress consensusSetting;
        NetworkAddress httpSetting;

        public NewParticipant(int id, String name , PubKey pubKey, PrivKey privKey, NetworkAddress httpSetting, NetworkAddress consensusSetting) {
            this.id = id;
            this.name = name;
            this.pubKey = pubKey;
            this.privKey = privKey;
            this.consensusSetting = consensusSetting;
            this.httpSetting = httpSetting;
        }

        public NetworkAddress getConsensusSetting() {
            return consensusSetting;
        }

        public PubKey getPubKey() {
            return pubKey;
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }

        public PrivKey getPrivKey() {
            return privKey;
        }

        public NetworkAddress getHttpSetting() {
            return httpSetting;
        }
    }
}
