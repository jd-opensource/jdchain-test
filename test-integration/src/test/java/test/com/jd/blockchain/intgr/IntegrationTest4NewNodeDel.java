package test.com.jd.blockchain.intgr;

import com.jd.blockchain.crypto.AddressEncoding;
import com.jd.blockchain.crypto.AsymmetricKeypair;
import com.jd.blockchain.crypto.HashDigest;
import com.jd.blockchain.crypto.KeyGenUtils;
import com.jd.blockchain.crypto.PrivKey;
import com.jd.blockchain.crypto.PubKey;
import com.jd.blockchain.gateway.GatewayConfigProperties;
import com.jd.blockchain.ledger.BlockchainIdentityData;
import com.jd.blockchain.ledger.BlockchainKeyGenerator;
import com.jd.blockchain.ledger.BlockchainKeypair;
import com.jd.blockchain.ledger.PreparedTransaction;
import com.jd.blockchain.ledger.TransactionResponse;
import com.jd.blockchain.ledger.TransactionState;
import com.jd.blockchain.ledger.TransactionTemplate;
import com.jd.blockchain.ledger.core.LedgerQuery;
import com.jd.blockchain.sdk.BlockchainService;
import com.jd.blockchain.sdk.client.GatewayServiceFactory;
import com.jd.blockchain.storage.service.DbConnectionFactory;
import com.jd.blockchain.test.PeerServer;
import com.jd.blockchain.tools.initializer.LedgerBindingConfig;
import com.jd.blockchain.utils.concurrent.ThreadInvoker;
import com.jd.blockchain.utils.http.converters.JsonResponseConverter;
import com.jd.blockchain.utils.net.NetworkAddress;
import com.jd.blockchain.utils.web.model.WebResponse;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Before;
import org.junit.Test;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeTest;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeWeb4Nodes;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeWeb5Nodes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static test.com.jd.blockchain.intgr.IntegrationBase.KeyPairResponse;
import static test.com.jd.blockchain.intgr.IntegrationBase.buildLedgers;
import static test.com.jd.blockchain.intgr.IntegrationBase.loadBindingConfig;
import static test.com.jd.blockchain.intgr.LedgerInitConsensusConfig.rocksdbConnectionStrings;

public class IntegrationTest4NewNodeDel {

    static String NEW_PUB1 = "3snPdw7i7PkdgqiGX7GbZuFSi1cwZn7vtjw4vifb1YoXgr9k6Kfmis";
    static String NEW_PRIV1 = "177gjtZu8w1phqHFVNiFhA35cfimXmP6VuqrBFhfbXBWK8s4TRwro2tnpffwP1Emwr6SMN6";

    static String NEW_PUB2 = "3snPdw7i7PkypgzJaKpXngtTdRqKX3MrEBcnhmRMzXYfduTT124pWk";
    static String NEW_PRIV2 = "177gjyhZvhR8dYKavXpaxJsKctc8Z7etmCcX7wsmcfGARFTZd46DU6AzX3eRuHfKCLq1bHy";

    static String NEW_PUB3 = "3snPdw7i7PmpKoEXV6kuomV5bAb1kaVjn7HBjcMucXNA4dQrvsVSxn";
    static String NEW_PRIV3 = "177gk1ZuTtEe2bDBZKuBkp5if2tt2TTXurgX8tfjTNnVNLRgGB8AjK9ZGweTRpnebjEXqrg";

    static String NEW_PUB4 = "3snPdw7i7Pf5o3aw6zFG7XC41t8eZtz6ahP8veE3uU8rUzbxpU3aej";
    static String NEW_PRIV4 = "177gjxw61bQ8hZfcq4MPBxcvmo1WkrGyiVY2Fo833yCbRKpY8xBH1TZKu5JKMZsYeRs7inf";


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

    public static PubKey pubKey0 = KeyGenUtils.decodePubKey(PUB_KEYS[0]);
    public static PubKey pubKey1 = KeyGenUtils.decodePubKey(PUB_KEYS[1]);
    public static PubKey pubKey2 = KeyGenUtils.decodePubKey(PUB_KEYS[2]);
    public static PubKey pubKey3 = KeyGenUtils.decodePubKey(PUB_KEYS[3]);

    public static PrivKey new_privkey1 = KeyGenUtils.decodePrivKeyWithRawPassword(NEW_PRIV1, PASSWORD);
    public static PrivKey new_privkey2 = KeyGenUtils.decodePrivKeyWithRawPassword(NEW_PRIV2, PASSWORD);
    public static PrivKey new_privkey3 = KeyGenUtils.decodePrivKeyWithRawPassword(NEW_PRIV3, PASSWORD);
    public static PrivKey new_privkey4 = KeyGenUtils.decodePrivKeyWithRawPassword(NEW_PRIV4, PASSWORD);

    public static PubKey new_pubKey1 = KeyGenUtils.decodePubKey(NEW_PUB1);
    public static PubKey new_pubKey2 = KeyGenUtils.decodePubKey(NEW_PUB2);
    public static PubKey new_pubKey3 = KeyGenUtils.decodePubKey(NEW_PUB3);
    public static PubKey new_pubKey4 = KeyGenUtils.decodePubKey(NEW_PUB4);

    public static String[] rocksdbConnectionStrings2 = new String[8];

    public static String[] rocksdbDirStrings2 = new String[8];

    public static final String DB_TYPE_ROCKSDB = "rocksdb";

    public static final  String  BFTSMART_PROVIDER = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";

    public static LedgerInitConsensusConfig.ConsensusConfig consensusConfig = new LedgerInitConsensusConfig.ConsensusConfig();

    private HashDigest ledgerHash;

    private static final String NEW_NODE_HOST = "127.0.0.1";
    private static final int NEW_NODE_HTTP_PORT1 = 12040;
    private static final int NEW_NODE_HTTP_PORT2 = 12050;
    private static final int NEW_NODE_HTTP_PORT3 = 12060;

    private static final int NEW_NODE_CONSENSUS_PORT1 = 22000;
    private static final int NEW_NODE_CONSENSUS_PORT2 = 20010;
    private static final int NEW_NODE_CONSENSUS_PORT3 = 20020;

    private static final int GATEWAY_MANAGER_PORT1 = 11000;

    private static final int GATEWAY_MANAGER_PORT2 = 11010;

    private static final int NEW_NODE_ID4 = 4;
    private static final int NEW_NODE_ID5 = 5;

    private NewParticipant peer0, peer1, peer2;

    private NewParticipant newParticipant1;

    private NewParticipant newParticipant2;

    private NewParticipant newParticipant3;


    @Before
    public void init() throws Exception {
        for (int i = 0; i < 11; i++) {
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

        peer0 = new NewParticipant(0, "pper0", pubKey0, privkey0, new NetworkAddress(NEW_NODE_HOST, 12000), new NetworkAddress(NEW_NODE_HOST, 8910));

        peer1 = new NewParticipant(1, "pper1", pubKey1, privkey1, new NetworkAddress(NEW_NODE_HOST, 12010), new NetworkAddress(NEW_NODE_HOST, 8920));

        peer2 = new NewParticipant(2, "pper2", pubKey2, privkey2, new NetworkAddress(NEW_NODE_HOST, 12020), new NetworkAddress(NEW_NODE_HOST, 8930));

        newParticipant1 = new NewParticipant(4, "peer4", new_pubKey1, new_privkey1, new NetworkAddress(NEW_NODE_HOST, NEW_NODE_HTTP_PORT1), new NetworkAddress(NEW_NODE_HOST, NEW_NODE_CONSENSUS_PORT1));

        newParticipant2 = new NewParticipant(5, "peer5", new_pubKey2, new_privkey2, new NetworkAddress(NEW_NODE_HOST, NEW_NODE_HTTP_PORT2), new NetworkAddress(NEW_NODE_HOST, NEW_NODE_CONSENSUS_PORT2));

        newParticipant3 = new NewParticipant(6, "peer6", new_pubKey3, new_privkey3, new NetworkAddress(NEW_NODE_HOST, NEW_NODE_HTTP_PORT3), new NetworkAddress(NEW_NODE_HOST, NEW_NODE_CONSENSUS_PORT3));
    }

    // 依次添加三个新的参与方，涉及到F的改变，再任意移除一个非领导者节点的场景验证；
    @Test
    public void testCase0() {
        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed 1----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);

            // 注册新的参与方2
            registParticipantByGateway0(blockchainService, newParticipant2, ledgerHash);

            Thread.sleep(5000);
            System.out.println("----------Regist Participant2 Completed 2----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 5);
            System.out.println("---------- DataBase Copy To New Node Completed 2----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant2, 5);

            // 注册新的参与方3
            registParticipantByGateway0(blockchainService, newParticipant3, ledgerHash);

            Thread.sleep(5000);
            System.out.println("----------Regist Participant3 Completed 3----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 6);
            System.out.println("---------- DataBase Copy To New Node Completed 3----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant3, 6);

            Thread.sleep(5000);
            deActivePartiNode(peer1, ledgerHash);
            System.out.println("---------- Deactive peer1 Completed ----------");

            Thread.sleep(5000);
            deActivePartiNode(peer2, ledgerHash);
            System.out.println("---------- Deactive peer2 Completed ----------");

            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 验证依次动态入网两个新参与方，并移除新加入的两个参与方的场景
    @Test
    public void testCase1() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);

            // 注册新的参与方2
            registParticipantByGateway0(blockchainService, newParticipant2, ledgerHash);

            Thread.sleep(5000);
            System.out.println("----------Regist Participant2 Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 5);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant2, 5);

            Thread.sleep(5000);
            deActivePartiNode(newParticipant2, ledgerHash);

            Thread.sleep(5000);
            deActivePartiNode(newParticipant1, ledgerHash);

            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 验证依次动态入网两个新参与方，移除新加入的两个参与方, 再次激活两个新参与方的场景
    @Test
    public void testCase2() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);

            // 注册新的参与方2
            registParticipantByGateway0(blockchainService, newParticipant2, ledgerHash);

            Thread.sleep(5000);
            System.out.println("----------Regist Participant2 Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 5);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant2, 5);

            Thread.sleep(5000);
            deActivePartiNode(newParticipant2, ledgerHash);

            Thread.sleep(5000);
            deActivePartiNode(newParticipant1, ledgerHash);

            Thread.sleep(5000);
            activePartiNode(newParticipant1, ledgerHash, NEW_NODE_HOST, "12000");

            Thread.sleep(5000);
            activePartiNode(newParticipant2, ledgerHash, NEW_NODE_HOST, "12000");

            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 验证依次动态入网两个新参与方，移除新加入的两个参与方, 再次激活两个新参与方, 移除账本初始化时的非领导者参与方的场景
    @Test
    public void testCase3() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);

            // 注册新的参与方2
            registParticipantByGateway0(blockchainService, newParticipant2, ledgerHash);

            Thread.sleep(5000);
            System.out.println("----------Regist Participant2 Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 5);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant2, 5);

            Thread.sleep(5000);
            deActivePartiNode(newParticipant2, ledgerHash);

            Thread.sleep(5000);
            deActivePartiNode(newParticipant1, ledgerHash);

            Thread.sleep(5000);
            activePartiNode(newParticipant1, ledgerHash, NEW_NODE_HOST, "12000");

            Thread.sleep(5000);
            activePartiNode(newParticipant2, ledgerHash, NEW_NODE_HOST, "12000");

            Thread.sleep(5000);
            deActivePartiNode(peer1, ledgerHash);

            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
            registUserByExistGatewayWrapper(blockchainService);

            // 再次发送交易检查网关本地的视图配置能否正确更新
            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 在5个节点的情况下，不允许移除2个节点的场景验证；
    @Test
    public void testCase4() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);

            Thread.sleep(5000);
            deActivePartiNode(peer1, ledgerHash);

            Thread.sleep(5000);
            deActivePartiNode(peer2, ledgerHash);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 在4个节点的情况下，不允许移除节点的场景验证；
    @Test
    public void testCase5() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            Thread.sleep(5000);
            deActivePartiNode(peer2, ledgerHash);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 移除节点为领导者节点的场景验证
    @Test
    public void testCase6() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            Thread.sleep(5000);
            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);
            System.out.println("---------- Start And Active New Node Completed ----------");

            Thread.sleep(5000);
            deActivePartiNode(peer0, ledgerHash);

            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(5000);
            registUserByExistGatewayWrapper(blockchainService);
            registUserByExistGatewayWrapper(blockchainService);
            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 反复进行移除领导者节点，激活领导者节点操作的场景
    @Test
    public void testCase7() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            Thread.sleep(5000);
            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);
            System.out.println("---------- Start And Active New Node Completed ----------");

            Thread.sleep(5000);
            deActivePartiNode(peer0, ledgerHash);

            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理

            Thread.sleep(8000);
            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(5000);
            activePartiNode(peer0, ledgerHash, NEW_NODE_HOST, "12030");

            Thread.sleep(8000);
            deActivePartiNode(peer1, ledgerHash);

            activePartiNode(peer1, ledgerHash, NEW_NODE_HOST, "12030");

            Thread.sleep(5000);
            registUserByExistGatewayWrapper(blockchainService);
            registUserByExistGatewayWrapper(blockchainService);
            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 反复移除初始参与方节点的场景验证
    @Test
    public void testCase8() throws Exception {

        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed ----------");

            Thread.sleep(5000);
            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);
            System.out.println("---------- Start And Active New Node Completed ----------");

            for (int i = 0; i < 1; i++) {
                Thread.sleep(5000);
                deActivePartiNode(peer1, ledgerHash);

                // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
                registUserByExistGatewayWrapper(blockchainService);

                Thread.sleep(5000);
                activePartiNode(peer1, ledgerHash, NEW_NODE_HOST, "12000");
            }

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

    // 依次移除两个共识节点，并重新激活的场景
    @Test
    public void testCase9() {
        try {

            //账本初始化
            ledgerHash = initLedger4Nodes(rocksdbConnectionStrings);

            // 启动4个Peer节点
            PeerServer[] peerNodes = peerNodeStart4(ledgerHash, DB_TYPE_ROCKSDB);

            // 创建连接peer0的网关
            BlockchainService blockchainService = createBlockChainService(LedgerInitConsensusConfig.bftsmartProvider, peerNodes, GATEWAY_MANAGER_PORT1);

            // 注册新的参与方
            registParticipantByGateway0(blockchainService, newParticipant1, ledgerHash);

            Thread.sleep(5000);
            System.out.println("---------- Ledger Init And Regist Participant Completed ----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 4);
            System.out.println("---------- DataBase Copy To New Node Completed 1----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant1, 4);

            // 注册新的参与方2
            registParticipantByGateway0(blockchainService, newParticipant2, ledgerHash);

            Thread.sleep(5000);
            System.out.println("----------Regist Participant2 Completed 2----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 5);
            System.out.println("---------- DataBase Copy To New Node Completed 2----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant2, 5);

            // 注册新的参与方3
            registParticipantByGateway0(blockchainService, newParticipant3, ledgerHash);

            Thread.sleep(5000);
            System.out.println("----------Regist Participant3 Completed 3----------");

            // 手动复制账本
            copyRocksdbToNewNode(0, 6);
            System.out.println("---------- DataBase Copy To New Node Completed 3----------");

            startNewPeerAndActive(ledgerHash, DB_TYPE_ROCKSDB, newParticipant3, 6);

            Thread.sleep(5000);
            deActivePartiNode(peer2, ledgerHash);
            System.out.println("---------- Deactive peer1 Completed ----------");

            Thread.sleep(5000);
            deActivePartiNode(newParticipant3, ledgerHash);

            Thread.sleep(5000);
            activePartiNode(peer2, ledgerHash, NEW_NODE_HOST, "12000");

//            // 通过老的网关0，发送交易，由于网关没有重新接入，获得的视图ID是0，没有更新，此时发送的交易到了共识节点一定会被特殊处理
//            registUserByExistGatewayWrapper(blockchainService);

            Thread.sleep(Integer.MAX_VALUE);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void registUserByExistGatewayWrapper(BlockchainService blockchainService) {

        TransactionResponse txResp = registUserByExistGateway(blockchainService);

        assertEquals(txResp.getExecutionState(), TransactionState.SUCCESS);

        System.out.println("---------- After Add New Node, Commit Tx By Old Gateway Completed----------");
    }

    private WebResponse startNewPeerAndActive(HashDigest ledgerHash, String dbTypeRocksdb, NewParticipant newParticipant, int id) throws InterruptedException {
        WebResponse webResponse;
        // 启动一个新的参与方，此时只启动HTTP服务，共识服务未开启
        startNewPeerNode(ledgerHash, DB_TYPE_ROCKSDB, newParticipant, id);
        System.out.println("---------- New Node Start Http But Without Consensus Completed ----------");

        // 激活新参与方的共识状态，更新原有共识网络的视图ID，启动新的参与方共识
        Thread.sleep(5000);
        webResponse = activePartiNode(newParticipant, ledgerHash, NEW_NODE_HOST, "12000");
        System.out.println("---------- Active New Node And View Update Completed ----------");

        return webResponse;
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

        HashDigest transactionHash = ptx.getTransactionHash();

        ptx.sign(adminKey);

        // 提交并等待共识返回；
        TransactionResponse txResp = ptx.commit();

        KeyPairResponse keyPairResponse = new KeyPairResponse();
        keyPairResponse.keyPair = user;
        keyPairResponse.txResp = txResp;
        keyPairResponse.txHash = transactionHash;
        return txResp;

    }


    private WebResponse activePartiNode(NewParticipant participant, HashDigest ledgerHash, String remoteManageHost, String remoteManagePort) {

        System.out.println("Address = " + AddressEncoding.generateAddress(participant.getPubKey()));

        String url = "http://" + participant.httpSetting.getHost() + ":" + participant.httpSetting.getPort() + "/management/delegate/activeparticipant";

        System.out.println("url = " + url);

        HttpPost httpPost = new HttpPost(url);

        List<BasicNameValuePair> para=new ArrayList<BasicNameValuePair>();

        BasicNameValuePair base58LedgerHash = new BasicNameValuePair("ledgerHash", ledgerHash.toBase58());
        BasicNameValuePair host = new BasicNameValuePair("consensusHost",  participant.getConsensusSetting().getHost());
        BasicNameValuePair port = new BasicNameValuePair("consensusPort",  String.valueOf(participant.getConsensusSetting().getPort()));

        // 指定已经启动的其他共识节点的HTTP管理端口
        BasicNameValuePair manageHost = new BasicNameValuePair("remoteManageHost",  remoteManageHost);
        BasicNameValuePair managePort = new BasicNameValuePair("remoteManagePort", remoteManagePort);


        para.add(base58LedgerHash);
        para.add(host);
        para.add(port);
        para.add(manageHost);
        para.add(managePort);

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(para,"UTF-8"));
            HttpClient httpClient = HttpClients.createDefault();

            HttpResponse response = httpClient.execute(httpPost);

            JsonResponseConverter jsonConverter = new JsonResponseConverter(WebResponse.class);

            WebResponse webResponse = (WebResponse) jsonConverter.getResponse(null, response.getEntity().getContent(), null);

            return webResponse;

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Active participant post request error!");
        }

        return null;
    }

    private WebResponse deActivePartiNode(NewParticipant participant, HashDigest ledgerHash) {

        String deactiveAddress = AddressEncoding.generateAddress(participant.pubKey).toBase58();

        System.out.println("Deactive Address = " + deactiveAddress);

        String url = "http://" + participant.httpSetting.getHost() + ":" + participant.httpSetting.getPort() + "/management/delegate/deactiveparticipant";

        System.out.println("url = " + url);

        HttpPost httpPost = new HttpPost(url);

        List<BasicNameValuePair> para=new ArrayList<BasicNameValuePair>();

        BasicNameValuePair base58LedgerHash = new BasicNameValuePair("ledgerHash", ledgerHash.toBase58());

        BasicNameValuePair deactiveParticipantAddress = new BasicNameValuePair("participantAddress", deactiveAddress);

        // 指定已经启动的其他共识节点的HTTP管理端口
        BasicNameValuePair manageHost = new BasicNameValuePair("remoteManageHost",  "127.0.0.1");
        BasicNameValuePair managePort = new BasicNameValuePair("remoteManagePort", "12000");


        para.add(base58LedgerHash);
        para.add(deactiveParticipantAddress);
        para.add(manageHost);
        para.add(managePort);

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(para,"UTF-8"));
            HttpClient httpClient = HttpClients.createDefault();

            HttpResponse response = httpClient.execute(httpPost);

            JsonResponseConverter jsonConverter = new JsonResponseConverter(WebResponse.class);

            WebResponse webResponse = (WebResponse) jsonConverter.getResponse(null, response.getEntity().getContent(), null);

            if (!webResponse.isSuccess()) {
                System.out.println("error result : " + webResponse.getError().getErrorMessage());
            }
            return webResponse;

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Deactive participant post request error!");
        }

        return null;
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


    public static PeerServer startNewPeerNode(HashDigest ledgerHash, String dbType, NewParticipant newParticipant, int id) {

        NetworkAddress peerSrvAddr = newParticipant.getHttpSetting();
        LedgerBindingConfig bindingConfig = loadBindingConfig(id, ledgerHash, dbType);
        PeerServer peer = new PeerServer(peerSrvAddr, bindingConfig);

        ThreadInvoker.AsyncCallback<Object> peerStarting = peer.start();

        peerStarting.waitReturn();

        return peer;
    }

    public BlockchainService createBlockChainService(String[] providers, PeerServer[] peerNodes, int gatewayPort) {
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
                providers,null, peerNodes[0].getServiceAddress());

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

        txTpl.participants().register(newParticipant.getName(), new BlockchainIdentityData(newParticipant.getPubKey()));

        // 签名；
        PreparedTransaction ptx = txTpl.prepare();

        HashDigest transactionHash = ptx.getTransactionHash();

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
