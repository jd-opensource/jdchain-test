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
import com.jd.blockchain.ledger.DataType;
import com.jd.blockchain.ledger.Event;
import com.jd.blockchain.ledger.SystemEvent;
import com.jd.blockchain.ledger.core.LedgerQuery;
import com.jd.blockchain.sdk.SystemEventListener;
import com.jd.blockchain.sdk.SystemEventPoint;
import com.jd.blockchain.sdk.UserEventListener;
import com.jd.blockchain.sdk.BlockchainService;
import com.jd.blockchain.sdk.EventContext;
import com.jd.blockchain.sdk.EventListenerHandle;
import com.jd.blockchain.sdk.UserEventPoint;
import com.jd.blockchain.sdk.client.GatewayServiceFactory;
import com.jd.blockchain.storage.service.DbConnectionFactory;
import com.jd.blockchain.test.PeerServer;
import com.jd.blockchain.tools.initializer.LedgerBindingConfig;

import org.junit.Test;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeTest;
import test.com.jd.blockchain.intgr.initializer.LedgerInitializeWeb4Nodes;
import utils.Bytes;
import utils.concurrent.ThreadInvoker;
import utils.io.BytesUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static test.com.jd.blockchain.intgr.IntegrationBase.*;
import static test.com.jd.blockchain.intgr.IntegrationTest4Bftsmart.DB_TYPE_ROCKSDB;

public class IntegrationTest4Operations {

    private static final String DB_TYPE_MEM = "mem";

    private static final String BFTSMART_PROVIDER = "com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider";

    private static final int NEW_USER_SIZE = 10;

    private static final int NEW_KV_SIZE = 10;

    private static final int NEW_EVENT_ACCOUNT_SIZE = 10;
    private static final int NEW_EVENT_NAME_SIZE = 10;
    private static final int NEW_EVENT_SEQUENCE_SIZE = 10;

    private boolean isRegisterUser = false;

    private boolean isRegisterDataAccount = false;

    private boolean isRegisterParticipant = false;

    private boolean isParticipantStateUpdate = false;

    private boolean isRegisterUserEventAccount = false;

    private boolean isPublishUserEvent = false;

    private boolean testUserEventListener = false;

    private boolean isWriteKv = false;

    @Test
    public void test() throws InterruptedException {
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void test4Rocksdb() throws InterruptedException {
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_ROCKSDB, LedgerInitConsensusConfig.rocksdbConnectionStrings);
    }

    @Test
    public void testUserRegister() throws InterruptedException {
        isRegisterUser = true;
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void testDataAccountRegister() throws InterruptedException {
        isRegisterDataAccount = true;
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void testWriteKv() throws InterruptedException {
        isRegisterDataAccount = true;
        isWriteKv = true;
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void testParticipantRegister() throws InterruptedException {
        isRegisterParticipant = true;
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void testParticipantUse() throws InterruptedException {
        isRegisterParticipant = true;
        isParticipantStateUpdate = true;
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void testUserEventAccountRegister() throws InterruptedException {
        isRegisterUserEventAccount = true;
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void testPublishUserEvent() throws InterruptedException {
        isRegisterUserEventAccount = true;
        isPublishUserEvent = true;
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void testEventListener() throws InterruptedException {
        isRegisterUserEventAccount = true;
        isPublishUserEvent = true;
        testUserEventListener = true;
        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    @Test
    public void testAll() throws InterruptedException {
        isRegisterUser = true;
        isRegisterDataAccount = true;
        isRegisterParticipant = true;
        isParticipantStateUpdate = true;
        isWriteKv = true;
        isRegisterUserEventAccount = true;
        isPublishUserEvent = true;
        testUserEventListener = true;

        test(LedgerInitConsensusConfig.bftsmartProvider, DB_TYPE_MEM, LedgerInitConsensusConfig.memConnectionStrings);
    }

    public void test(String[] providers, String dbType, String[] dbConnections) throws InterruptedException {

        final ExecutorService sendReqExecutors = Executors.newFixedThreadPool(20);

        // 内存账本初始化
        HashDigest ledgerHash = initLedger(dbConnections);

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
        IntegrationBase.testConsistencyAmongNodes(ledgers);

        LedgerQuery ledgerRepository = ledgers[0];
        GatewayServiceFactory gwsrvFact = GatewayServiceFactory.connect(gateway.getServiceAddress());
        PrivKey privkey0 = KeyGenUtils.decodePrivKeyWithRawPassword(IntegrationBase.PRIV_KEYS[0], IntegrationBase.PASSWORD);
        PubKey pubKey0 = KeyGenUtils.decodePubKey(IntegrationBase.PUB_KEYS[0]);
        AsymmetricKeypair adminKey = new AsymmetricKeypair(pubKey0, privkey0);

        BlockchainService blockchainService = gwsrvFact.getBlockchainService();

        if (isRegisterUser) {
            CountDownLatch countDownLatch = new CountDownLatch(NEW_USER_SIZE);
            for (int i = 0; i < NEW_USER_SIZE; i++) {
                sendReqExecutors.execute(() -> {

                    System.out.printf(" sdk execute time = %s threadId = %s \r\n", System.currentTimeMillis(), Thread.currentThread().getId());
                    IntegrationBase.testSDK_RegisterUser(adminKey, ledgerHash, blockchainService);
                    countDownLatch.countDown();
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (isRegisterDataAccount) {
            IntegrationBase.KeyPairResponse dataAccountResponse = IntegrationBase.testSDK_RegisterDataAccount(adminKey, ledgerHash, blockchainService);
            validKeyPair(dataAccountResponse, ledgerRepository, IntegrationBase.KeyPairType.DATAACCOUNT);
            if (isWriteKv) {
                for (int m = 0; m < NEW_KV_SIZE; m++) {
                    BlockchainKeypair da = dataAccountResponse.keyPair;
                    IntegrationBase.KvResponse kvResponse = IntegrationBase.testSDK_InsertData(adminKey, ledgerHash, blockchainService, da.getAddress());
                    validKvWrite(kvResponse, ledgerRepository, blockchainService);
                }
            }
        }

        if (isRegisterParticipant) {
            long participantCount = ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipantCount();
            long userCount = ledgerRepository.getUserAccountSet(ledgerRepository.retrieveLatestBlock()).getTotal();

            System.out.printf("before add participant: participantCount = %d, userCount = %d\r\n", (int) participantCount, (int) userCount);
            IntegrationBase.KeyPairResponse participantResponse = IntegrationBase.testSDK_RegisterParticipant(adminKey, ledgerHash, blockchainService);
            ;
            participantCount = ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipantCount();
            userCount = ledgerRepository.getUserAccountSet(ledgerRepository.retrieveLatestBlock()).getTotal();
            System.out.printf("after add participant: participantCount = %d, userCount = %d\r\n", (int) participantCount, (int) userCount);

            BftsmartConsensusViewSettings consensusSettings = (BftsmartConsensusViewSettings) ConsensusProviders.getProvider(BFTSMART_PROVIDER).getSettingsFactory().getConsensusSettingsEncoder().decode(ledgerRepository.getAdminInfo().getSettings().getConsensusSetting().toBytes());
            System.out.printf("update participant state before ,old consensus env node num = %d\r\n", consensusSettings.getNodes().length);

            for (int i = 0; i < participantCount; i++) {
                System.out.printf("part%d state = %d\r\n", i, ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipants()[i].getParticipantNodeState().CODE);
            }
            if (isParticipantStateUpdate) {
                IntegrationBase.testSDK_UpdateParticipantState(adminKey, new BlockchainKeypair(participantResponse.getKeyPair().getPubKey(), participantResponse.getKeyPair().getPrivKey()), ledgerHash, blockchainService);
                BftsmartConsensusViewSettings consensusSettingsNew = (BftsmartConsensusViewSettings) ConsensusProviders.getProvider(BFTSMART_PROVIDER).getSettingsFactory().getConsensusSettingsEncoder().decode(ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getSettings().getConsensusSetting().toBytes());

                System.out.printf("update participant state after ,new consensus env node num = %d\r\n", consensusSettingsNew.getNodes().length);
                for (int i = 0; i < participantCount; i++) {
                    System.out.printf("part%d state = %d\r\n", i, ledgerRepository.getAdminInfo(ledgerRepository.retrieveLatestBlock()).getParticipants()[i].getParticipantNodeState().CODE);
                }
            }
        }

        if (isRegisterUserEventAccount) {
            Bytes[] eventAddress = new Bytes[NEW_EVENT_ACCOUNT_SIZE];
            for (int m = 0; m < NEW_EVENT_ACCOUNT_SIZE; m++) {
                KeyPairResponse response = IntegrationBase.testSDK_RegisterEventAccount(adminKey, ledgerHash, blockchainService);
                Thread.sleep(100);
                validKeyPair(response, ledgerRepository, IntegrationBase.KeyPairType.EVENTACCOUNT);
                eventAddress[m] = response.keyPair.getAddress();
            }

            Map<Bytes, String[]> eventNames = new HashMap<>();
            for (int m = 0; m < NEW_EVENT_ACCOUNT_SIZE; m++) {
                for (int n = 0; n < NEW_EVENT_NAME_SIZE; n++) {
                    if (n == 0) {
                        eventNames.put(eventAddress[m], new String[NEW_EVENT_NAME_SIZE]);
                    }
                    eventNames.get(eventAddress[m])[n] = "event" + n;
                }
            }

            BlockchainNewBlockEventListener blockListener = null;
            EventListenerHandle blockListenerHandler = null;
            // 监听事件
            if (testUserEventListener) {
                for (int s = 0; s < NEW_EVENT_SEQUENCE_SIZE; s++) {
                    // 单个账户单个事件名
                    BlockchainUserEventListener a1e1Listener = new BlockchainUserEventListener();
                    a1e1Listener.register(eventAddress[0], eventNames.get(eventAddress[0])[0], NEW_EVENT_SEQUENCE_SIZE - 1, NEW_EVENT_SEQUENCE_SIZE - s);
                    blockchainService.monitorUserEvent(ledgerHash, eventAddress[0].toBase58(), eventNames.get(eventAddress[0])[0], s, a1e1Listener);
                    // 单个账户多个事件名
                    BlockchainUserEventListener a1emListener = new BlockchainUserEventListener();
                    UserEventPoint[] a1emEventPoints = new UserEventPoint[eventNames.size()];
                    for (int m = 0; m < eventNames.get(eventAddress[0]).length; m++) {
                        a1emEventPoints[m] = new UserEventPoint(eventAddress[0].toBase58(), eventNames.get(eventAddress[0])[m], s);
                        a1emListener.register(eventAddress[0], eventNames.get(eventAddress[0])[m], NEW_EVENT_SEQUENCE_SIZE - 1, NEW_EVENT_SEQUENCE_SIZE - s);
                    }
                    blockchainService.monitorUserEvent(ledgerHash, a1emEventPoints, a1emListener);
                    // 多个账户多个事件名
                    BlockchainUserEventListener amemListener = new BlockchainUserEventListener();
                    UserEventPoint[] amemEventPoints = new UserEventPoint[eventNames.size() * eventNames.get(eventAddress[0]).length];
                    int m = 0;
                    for (Map.Entry<Bytes, String[]> kv : eventNames.entrySet()) {
                        for (int n = 0; n < kv.getValue().length; n++) {
                            amemEventPoints[m * kv.getValue().length + n] = new UserEventPoint(kv.getKey().toBase58(), kv.getValue()[n], s);
                            amemListener.register(kv.getKey(), kv.getValue()[n], NEW_EVENT_SEQUENCE_SIZE - 1, NEW_EVENT_SEQUENCE_SIZE - s);
                        }
                        m++;
                    }
                    blockchainService.monitorUserEvent(ledgerHash, amemEventPoints, amemListener);
                }

                blockListener = new BlockchainNewBlockEventListener();
                blockListenerHandler = blockchainService.monitorSystemEvent(ledgerHash, SystemEvent.NEW_BLOCK_CREATED, 0, blockListener);
            }

            // 发布事件
            if (isPublishUserEvent) {
                for (Map.Entry<Bytes, String[]> kv : eventNames.entrySet()) {
                    for (int m = 0; m < kv.getValue().length; m++) {
                        Bytes da = kv.getKey();
                        for (int j = -1; j < NEW_EVENT_SEQUENCE_SIZE - 1; j++) {
                            IntegrationBase.EventResponse eventResponse = IntegrationBase.testSDK_PublishEvent(adminKey, ledgerHash, blockchainService, da, kv.getValue()[m], j);
                            Thread.sleep(100);
                            validEventPublish(eventResponse, ledgerRepository, blockchainService);
                        }
                    }
                }
            }

            while (blockListener != null) {
                Thread.sleep(1000);
                if(blockListener.height.get() == blockchainService.getLedger(ledgerHash).getLatestBlockHeight()) {
                    blockListenerHandler.cancel();
                    break;
                }
            }
        }

        System.out.println("----------------- Init Completed -----------------");
        IntegrationBase.testConsistencyAmongNodes(ledgers);
//        Thread.sleep(Integer.MAX_VALUE);

    }
    private HashDigest initLedger(String[] dbConnections) {
        LedgerInitializeWeb4Nodes ledgerInit = new LedgerInitializeWeb4Nodes();
        HashDigest ledgerHash = ledgerInit.testInitWith4Nodes(LedgerInitConsensusConfig.bftsmartConfig, dbConnections);
        System.out.printf("LedgerHash = %s \r\n", ledgerHash.toBase58());
        return ledgerHash;
    }

    class BlockchainUserEventListener implements UserEventListener<UserEventPoint> {

        int totalCount = 0;
        AtomicInteger count = new AtomicInteger(0);

        Map<Bytes, Map<String, Long>> points = new HashMap<>();
        Map<Bytes, Map<String, Boolean>> status = new HashMap<>();

        public void register(Bytes eventAccount, String eventName, long maxSequence, int count) {
            if (!points.containsKey(eventAccount)) {
                points.put(eventAccount, new HashMap<>());
                status.put(eventAccount, new ConcurrentHashMap<>());
            }

            if (!points.get(eventAccount).containsKey(eventName)) {
                points.get(eventAccount).put(eventName, maxSequence);
                status.get(eventAccount).put(eventName, false);
            }

            totalCount += count;
        }

        @Override
        public void onEvent(Event eventMessage, EventContext<UserEventPoint> eventContext) {
            count.incrementAndGet();
            Object content;
            if (eventMessage.getContent().getType() == DataType.TEXT) {
                content = eventMessage.getContent().getBytes().toUTF8String();
            } else {
                content = BytesUtils.toLong(eventMessage.getContent().getBytes().toBytes());
            }
//            System.out.println("eventAccount:" + eventMessage.getEventAccount().toBase58() + ", name:" + eventMessage.getName() + ", sequence:" + eventMessage.getSequence() + ", content:" + content);
            assertTrue(eventMessage.getSequence() <= points.get(eventMessage.getEventAccount()).get(eventMessage.getName()));
            if (eventMessage.getSequence() == points.get(eventMessage.getEventAccount()).get(eventMessage.getName())) {
                status.get(eventMessage.getEventAccount()).put(eventMessage.getName(), true);
            }
            if(finish()) {
                assertEquals(count.intValue(), totalCount);
                eventContext.getHandle().cancel();
            }
        }

        private boolean finish() {
            boolean finish = true;
            for(Map.Entry<Bytes, Map<String, Boolean>> kv : status.entrySet()) {
                for(boolean status : kv.getValue().values()) {
                    finish = finish && status;
                    if(!finish) {
                        return false;
                    }
                }
            }

            return true;
        }
    }

    class BlockchainNewBlockEventListener implements SystemEventListener<SystemEventPoint> {

        private AtomicInteger height = new AtomicInteger(-1);

        @Override
        public void onEvents(Event[] eventMessages, EventContext<SystemEventPoint> eventContext) {
            for(Event eventMessage : eventMessages) {
                height.incrementAndGet();
                System.out.println("name:" + eventMessage.getName() + ", sequence:" + eventMessage.getSequence() + ", content:" + BytesUtils.toLong(eventMessage.getContent().getBytes().toBytes()));
            }
        }
    }
}
