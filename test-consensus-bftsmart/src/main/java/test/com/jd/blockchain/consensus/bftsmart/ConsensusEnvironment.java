package test.com.jd.blockchain.consensus.bftsmart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;

import com.jd.blockchain.consensus.ClientIdentification;
import com.jd.blockchain.consensus.ClientIncomingSettings;
import com.jd.blockchain.consensus.ConsensusProvider;
import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.ConsensusViewSettings;
import com.jd.blockchain.consensus.Replica;
import com.jd.blockchain.consensus.bftsmart.BftsmartConsensusProvider;
import com.jd.blockchain.consensus.bftsmart.BftsmartConsensusSettings;
import com.jd.blockchain.consensus.bftsmart.BftsmartNodeSettings;
import com.jd.blockchain.consensus.bftsmart.BftsmartReplica;
import com.jd.blockchain.consensus.client.ClientSettings;
import com.jd.blockchain.consensus.client.ConsensusClient;
import com.jd.blockchain.consensus.manage.ConsensusManageClient;
import com.jd.blockchain.consensus.manage.ConsensusView;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.NodeServer;
import com.jd.blockchain.consensus.service.ServerSettings;
import com.jd.blockchain.consensus.service.StateMachineReplicate;
import com.jd.blockchain.crypto.AsymmetricKeypair;
import com.jd.blockchain.crypto.Crypto;
import com.jd.blockchain.crypto.service.classic.ClassicAlgorithm;
import com.jd.blockchain.utils.AbstractSkippingIterator;
import com.jd.blockchain.utils.ArrayUtils;
import com.jd.blockchain.utils.ConsoleUtils;
import com.jd.blockchain.utils.PropertiesUtils;
import com.jd.blockchain.utils.SkippingIterator;
import com.jd.blockchain.utils.concurrent.AsyncFuture;
import com.jd.blockchain.utils.net.NetworkAddress;

/**
 * {@link ConsensusEnvironment} 表示由一组共识节点组成的共识网络以及一组对应的共识客户端一起构成的共识网络环境；
 * 
 * @author huanghaiquan
 *
 */
public class ConsensusEnvironment {

	private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

	private final ConsensusProvider CS_PROVIDER;

	private ConsensusViewSettings viewSettings;

	private String realmName;

	private Replica[] replicas;

	private volatile NodeServer[] nodeServers;

	private List<ConsensusClient> clients = new LinkedList<ConsensusClient>();

	private volatile MessageHandlerDelegater[] messageDelegaters;
	private StateMachineReplicate[] stateMachineReplicaters;

	public String gerProviderName() {
		return CS_PROVIDER.getName();
	}

	public int getReplicaCount() {
		return replicas.length;
	}

	public SkippingIterator<ReplicaNodeServer> getNodes() {
		return new AbstractSkippingIterator<ReplicaNodeServer>() {

			@Override
			public long getTotalCount() {
				return replicas.length;
			}

			@Override
			protected ReplicaNodeServer get(long cursor) {
				return new ReplicaNodeServer() {
					@Override
					public Replica getReplica() {
						return replicas[(int) cursor];
					}

					@Override
					public NodeServer getNodeServer() {
						return nodeServers[(int) cursor];
					}
				};
			}
		};
	}

//	public MessageHandle[] getMessageHandlers() {
//		return messageHandlers == null ? null : messageHandlers.clone();
//	}

	public List<StateMachineReplicate> getStateMachineReplicaters() {
		return Arrays.asList(stateMachineReplicaters);
	}

	private ConsensusEnvironment(String realmName, ConsensusViewSettings csSettings, Replica[] replicas,
			StateMachineReplicate[] stateMachineReplicater, ConsensusProvider consensusProvider) {
		this.CS_PROVIDER = consensusProvider;

		this.realmName = realmName;
		this.replicas = replicas;
		this.viewSettings = csSettings;

		this.messageDelegaters = new MessageHandlerDelegater[replicas.length];
		for (int i = 0; i < messageDelegaters.length; i++) {
			messageDelegaters[i] = new MessageHandlerDelegater();
		}

		this.stateMachineReplicaters = stateMachineReplicater;
	}

	public String getRealmName() {
		return realmName;
	}

	public static ConsensusEnvironment setup_BFTSMaRT(String realmName, int nodeCount) throws IOException {
		// 端口从 10000 开始，每个递增 10 ；
		NetworkAddress[] nodesNetworkAddresses = createMultiPortsAddresses("127.0.0.1", nodeCount, 11600, 10);
		return setup_BFTSMaRT(realmName, nodesNetworkAddresses);
	}

	public static ConsensusEnvironment setup_BFTSMaRT(String realmName, NetworkAddress[] nodesNetworkAddresses)
			throws IOException {
		Properties consensusProperties = PropertiesUtils.loadProperties("classpath:bftsmart-test.config", "UTF-8");
		return setup_BFTSMaRT(realmName, consensusProperties, nodesNetworkAddresses);
	}

	public static ConsensusEnvironment setup_BFTSMaRT(String realmName, Properties consensusProperties,
			NetworkAddress[] nodesNetworkAddresses) {
		MessageHandle[] messageHandlers = new MessageHandle[nodesNetworkAddresses.length];
		for (int i = 0; i < messageHandlers.length; i++) {
			messageHandlers[i] = Mockito.mock(MessageHandle.class);
		}

		StateMachineReplicate[] smrs = new StateMachineReplicate[nodesNetworkAddresses.length];
		for (int i = 0; i < smrs.length; i++) {
			smrs[i] = Mockito.mock(StateMachineReplicate.class);
		}

		return setup_BFTSMaRT(realmName, consensusProperties, nodesNetworkAddresses, messageHandlers, smrs);
	}

	public static ConsensusEnvironment setup_BFTSMaRT(String realmName, String consensusConfig,
			NetworkAddress[] nodesNetworkAddresses) throws IOException {
		return setup_BFTSMaRT(realmName, consensusConfig, nodesNetworkAddresses, null);
	}

	public static ConsensusEnvironment setup_BFTSMaRT(String realmName, String consensusConfig,
			NetworkAddress[] nodesNetworkAddresses, MessageHandle[] messageHandler) throws IOException {
		Properties consensusProperties = PropertiesUtils.loadProperties(consensusConfig, "UTF-8");

		StateMachineReplicate[] smrs = new StateMachineReplicate[nodesNetworkAddresses.length];
		for (int i = 0; i < smrs.length; i++) {
			smrs[i] = Mockito.mock(StateMachineReplicate.class);
		}

		return setup_BFTSMaRT(realmName, consensusProperties, nodesNetworkAddresses, messageHandler, smrs);
	}

	public static ConsensusEnvironment setup_BFTSMaRT(String realmName, Properties consensusProperties,
			NetworkAddress[] nodesNetworkAddresses, MessageHandle[] messageHandlers) {
		StateMachineReplicate[] smrs = new StateMachineReplicate[nodesNetworkAddresses.length];
		for (int i = 0; i < smrs.length; i++) {
			smrs[i] = Mockito.mock(StateMachineReplicate.class);
		}

		return setup_BFTSMaRT(realmName, consensusProperties, nodesNetworkAddresses, messageHandlers, smrs);
	}

	public static ConsensusEnvironment setup_BFTSMaRT(String realmName, Properties consensusProperties,
			NetworkAddress[] nodesNetworkAddresses, MessageHandle messageHandler[], StateMachineReplicate[] smr) {
		// 节点总数；
		int nodeCount = nodesNetworkAddresses.length;

		// 创建副本信息；
		Replica[] replicas = createReplicaWithRandom(nodeCount);
		BftsmartConsensusSettings csSettings = buildConsensusSettings_BFTSMaRT(consensusProperties, replicas);
		csSettings = stubNetworkAddress(csSettings, nodesNetworkAddresses);

		return setup(realmName, csSettings, replicas, messageHandler, smr, BftsmartConsensusProvider.INSTANCE);
	}

	public static ConsensusEnvironment setup_BFTSMaRT(String realmName, Properties consensusProperties,
			NetworkAddress[] nodesNetworkAddresses, StateMachineReplicate[] smr) {
		// 节点总数；
		int nodeCount = nodesNetworkAddresses.length;

		// 创建副本信息；
		Replica[] replicas = createReplicaWithRandom(nodeCount);
		BftsmartConsensusSettings csSettings = buildConsensusSettings_BFTSMaRT(consensusProperties, replicas);
		csSettings = stubNetworkAddress(csSettings, nodesNetworkAddresses);

		return setup(realmName, csSettings, replicas, smr, BftsmartConsensusProvider.INSTANCE);
	}

	/**
	 * 建立共识网络；
	 * 
	 * @param realmName
	 * @param csSettings
	 * @param replicas
	 * @param messageHandler
	 * @param smr
	 * @param consensusProvider
	 * @return
	 */
	public static ConsensusEnvironment setup(String realmName, ConsensusViewSettings csSettings, Replica[] replicas,
			StateMachineReplicate[] smr, ConsensusProvider consensusProvider) {
		ConsensusEnvironment csEnv = new ConsensusEnvironment(realmName, csSettings, replicas, smr, consensusProvider);
		csEnv.installNodeServers();

		return csEnv;
	}

	/**
	 * 建立共识网络；
	 * 
	 * @param realmName
	 * @param csSettings
	 * @param replicas
	 * @param messageHandler
	 * @param smr
	 * @param consensusProvider
	 * @return
	 */
	public static ConsensusEnvironment setup(String realmName, ConsensusViewSettings csSettings, Replica[] replicas,
			MessageHandle[] messageHandlers, StateMachineReplicate[] smr, ConsensusProvider consensusProvider) {

		ConsensusEnvironment csEnv = new ConsensusEnvironment(realmName, csSettings, replicas, smr, consensusProvider);

		if (messageHandlers != null) {
			csEnv.delegateMessageHandlers(messageHandlers);
		}

		csEnv.installNodeServers();

		return csEnv;
	}

	// ----------------------------------------

	private void installNodeServers() {
		if (messageDelegaters == null) {
			return;
		}
		int nodeCount = replicas.length;
		if (nodeCount != messageDelegaters.length) {
			throw new IllegalArgumentException("The number of message handler and replica are not equal!");
		}
		NodeServer[] nodeServers = new NodeServer[nodeCount];
		for (int i = 0; i < nodeServers.length; i++) {
			nodeServers[i] = createNodeServer(realmName, viewSettings, replicas[i], messageDelegaters[i],
					stateMachineReplicaters[i], CS_PROVIDER);
		}
		this.nodeServers = nodeServers;
	}

	public boolean isRunning() {
		if (nodeServers != null) {
			for (NodeServer node : nodeServers) {
				if (node.isRunning()) {
					return true;
				}
			}
		}
		return false;
	}

	public void reinstallNodeServers() {
		if (nodeServers != null) {
			for (NodeServer node : nodeServers) {
				if (node.isRunning()) {
					throw new IllegalStateException("The current node servers has not stopped!");
				}
			}
		}
		this.nodeServers = null;

		installNodeServers();
	}

	public void reinstallNodeServers(MessageHandle[] messageHandlers) {
		if (messageHandlers == null) {
			throw new IllegalArgumentException("No messageHandlers!");
		}
		if (nodeServers != null) {
			for (NodeServer node : nodeServers) {
				if (node.isRunning()) {
					throw new IllegalStateException("The current node servers has not stopped!");
				}
			}
		}
		this.nodeServers = null;

		delegateMessageHandlers(messageHandlers);
		installNodeServers();
	}

	/**
	 * 设置节点的消息处理；
	 * 
	 * @param messageHandlers
	 */
	public void delegateMessageHandlers(MessageHandle[] messageHandlers) {
		if (messageHandlers == null) {
			throw new IllegalArgumentException("No message handler!");
		}
		if (messageHandlers.length != messageDelegaters.length) {
			throw new IllegalArgumentException("The number of message handlers is not equal to the number of nodes;");
		}
		for (int i = 0; i < messageHandlers.length; i++) {
			messageDelegaters[i].delegateTo(messageHandlers[i]);
		}
	}

	public void clearMessageHandlers() {
		for (int i = 0; i < messageDelegaters.length; i++) {
			messageDelegaters[i].clear();
		}
	}

	/**
	 * 加入新的参与方；
	 * <p>
	 * 
	 * 操作将建立参与方的共识节点，并更新当前已运行节点的共识网络视图；
	 * <p>
	 * 
	 * 在方法返回之前，新建的共识节点将会被启动；
	 * 
	 * @param replica
	 * @param netAddress
	 * @return
	 */
	public NodeServer joinReplica(Replica replica, NetworkAddress netAddress) {
		return joinReplica(replica, netAddress, null);
	}

	/**
	 * 加入新的参与方；
	 * <p>
	 * 
	 * 操作将建立参与方的共识节点，并更新当前已运行节点的共识网络视图；
	 * <p>
	 * 
	 * 在方法返回之前，新建的共识节点将会被启动；
	 * 
	 * @param replica
	 * @param netAddress
	 * @param messageHandler
	 * @return
	 */
	public NodeServer joinReplica(Replica replica, NetworkAddress netAddress, MessageHandle messageHandler) {
		BftsmartReplica bftsmartReplica = new BftsmartReplica(replica.getId(), netAddress, replica.getAddress(),
				replica.getPubKey());

		// 创建新的共识节点的视图配置信息；
		BftsmartConsensusSettings nextViewSettings = (BftsmartConsensusSettings) CS_PROVIDER.getSettingsFactory()
				.getConsensusSettingsBuilder().addReplicaSetting(viewSettings, bftsmartReplica);

		// 向现有的共识网络发起“加入节点”的共识请求；
		AsymmetricKeypair clientKey = Crypto.getSignatureFunction(ClassicAlgorithm.ED25519).generateKeypair();
		ClientIncomingSettings incomingSetting = authClientsFrom(nodeServers[0], clientKey, CS_PROVIDER);
		ClientSettings clientSettings = CS_PROVIDER.getClientFactory().buildClientSettings(incomingSetting);

		try (ConsensusManageClient manageClient = CS_PROVIDER.getManagerClientFactory()
				.setupManageClient(clientSettings)) {
			manageClient.connect();
			AsyncFuture<ConsensusView> future = manageClient.getManageService().addNode(bftsmartReplica);
			ConsensusView nextView = future.get();

			// 校验 view id 是否一致；
			if (nextView.getViewID() != nextViewSettings.getViewId()) {
				throw new IllegalStateException(
						"The next view id from remote consensus network is not equal to the next view id from local settings!");
			}
		}

		// 创建并启动新加入的节点；
		StateMachineReplicate smr = Mockito.mock(StateMachineReplicate.class);

		MessageHandlerDelegater messageDelegater = new MessageHandlerDelegater(messageHandler);
		NodeServer nodeServer = createNodeServer(realmName, nextViewSettings, bftsmartReplica, messageDelegater, smr,
				CS_PROVIDER);

		// 把新节点加入到上下文的节点列表；
		addNewNode(bftsmartReplica, nodeServer, messageDelegater, smr);

		// 启动节点；
		nodeServer.start();

		return nodeServer;
	}

	private void addNewNode(Replica replica, NodeServer nodeServer, MessageHandlerDelegater messageDelegater,
			StateMachineReplicate smr) {
		this.replicas = ArrayUtils.concat(this.replicas, replica, Replica.class);
		this.nodeServers = ArrayUtils.concat(this.nodeServers, nodeServer, NodeServer.class);

		this.messageDelegaters = ArrayUtils.concat(this.messageDelegaters, messageDelegater,
				MessageHandlerDelegater.class);
		this.stateMachineReplicaters = ArrayUtils.concat(this.stateMachineReplicaters, smr,
				StateMachineReplicate.class);
	}

	public void startNodeServers() {
		if (nodeServers == null) {
			throw new IllegalStateException("Node servers has not been setup!");
		}
		startNodeServers(nodeServers);
	}

	public void stopNodeServers() {
		if (nodeServers == null) {
			return;
		}
		stopNodeServers(nodeServers);
	}

	public ConsensusClient[] getClients() {
		return clients.toArray(new ConsensusClient[clients.size()]);
	}

	public ConsensusClient[] setupNewClients(int clientCount) throws ConsensusSecurityException {
		AsymmetricKeypair[] clientKeys = initRandomKeys(clientCount);

		ClientIncomingSettings[] clientSettings = authClientsFrom(nodeServers, clientKeys, CS_PROVIDER);

		ConsensusClient[] newClients = setupConsensusClients(clientSettings, CS_PROVIDER);
		for (int i = 0; i < newClients.length; i++) {
			this.clients.add(newClients[i]);
		}

		return newClients;
	}

	public ConsensusClient[] resetupClients(int clientCount) throws ConsensusSecurityException {
		closeAllClients();
		return setupNewClients(clientCount);
	}

	public void closeAllClients() {
		for (ConsensusClient cli : clients) {
			cli.close();
		}
		clients.clear();
	}

	/**
	 * 从指定节点服务器中认证客户端，返回客户端接入配置；
	 * 
	 * <p>
	 * 
	 * 对于参数中的每一个客户端密钥，从服务器列表中随机挑选一个进行认证；
	 * 
	 * <p>
	 * 
	 * 返回的客户端接入配置的数量和密钥的数量一致；
	 * 
	 * @param nodeServers
	 * @param clientKeys
	 * @return
	 * @throws ConsensusSecurityException
	 */
	private static ClientIncomingSettings[] authClientsFrom(NodeServer[] nodeServers, AsymmetricKeypair[] clientKeys,
			ConsensusProvider consensusProvider) throws ConsensusSecurityException {
		ClientIncomingSettings[] incomingSettings = new ClientIncomingSettings[clientKeys.length];

		Random rand = new Random();
		for (int i = 0; i < clientKeys.length; i++) {
			incomingSettings[i] = authClientsFrom(nodeServers[rand.nextInt(nodeServers.length)], clientKeys[i],
					consensusProvider);
		}

		return incomingSettings;
	}

	private static ClientIncomingSettings authClientsFrom(NodeServer nodeServer, AsymmetricKeypair clientKeys,
			ConsensusProvider consensusProvider) {
		ClientIdentification clientIdentification = consensusProvider.getClientFactory().buildAuthId(clientKeys);

		try {
			return nodeServer.getClientAuthencationService().authencateIncoming(clientIdentification);
		} catch (ConsensusSecurityException e) {
			throw new IllegalStateException("Fail to authencate client incoming! --" + e.getMessage(), e);
		}
	}

	private static ConsensusClient[] setupConsensusClients(ClientIncomingSettings[] clientIncomingSettings,
			ConsensusProvider consensusProvider) {
		ConsensusClient[] clients = new ConsensusClient[clientIncomingSettings.length];

		for (int i = 0; i < clients.length; i++) {
			ClientSettings clientSettings = consensusProvider.getClientFactory()
					.buildClientSettings(clientIncomingSettings[i]);
			clients[i] = consensusProvider.getClientFactory().setupClient(clientSettings);

			clients[i].connect();
		}

		return clients;
	}

	private static void startNodeServers(NodeServer[] nodeServers) {
		CountDownLatch startupLatch = new CountDownLatch(nodeServers.length);
		for (int i = 0; i < nodeServers.length; i++) {
			int id = i;
			NodeServer nodeServer = nodeServers[i];
			EXECUTOR_SERVICE.execute(new Runnable() {

				@Override
				public void run() {
					nodeServer.start();
					ConsoleUtils.info("Replica Node [%s : %s] started! ", id,
							nodeServer.getSettings().getReplicaSettings().getAddress());
					startupLatch.countDown();
				}
			});
		}

		try {
			startupLatch.await(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new IllegalStateException("Timeout occurred while waiting to complete the startup of all nodes!", e);
		}
		ConsoleUtils.info("All replicas start success!");
	}

	private static void stopNodeServers(NodeServer[] nodeServers) {

		List<NodeServer> runningNodes = new ArrayList<>();
		for (int i = 0; i < nodeServers.length; i++) {
			if (nodeServers[i].isRunning()) {
				runningNodes.add(nodeServers[i]);
			}
		}
		if (runningNodes.size() == 0) {
			return;
		}
		CountDownLatch startupLatch = new CountDownLatch(runningNodes.size());
		for (NodeServer nodeServer : runningNodes) {
			EXECUTOR_SERVICE.execute(new Runnable() {

				@Override
				public void run() {
					nodeServer.stop();
					ConsoleUtils.info("Replica [%s] stop! ", nodeServer.toString());
					startupLatch.countDown();
				}
			});
		}

		try {
			startupLatch.await(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new IllegalStateException("Timeout occurred while waiting to completed the stopping of all nodes!",
					e);
		}
		ConsoleUtils.info("All replicas stop!");
	}

	private static AsymmetricKeypair[] initRandomKeys(int clientCount) {
		AsymmetricKeypair[] keys = new AsymmetricKeypair[clientCount];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = Crypto.getSignatureFunction(ClassicAlgorithm.ED25519).generateKeypair();
		}
		return keys;
	}

	public static Replica[] createReplicaWithRandom(int n) {
		Replica[] replicas = new Replica[n];
		for (int i = 0; i < replicas.length; i++) {
			replicas[i] = createReplicaWithRandom(i, "节点[" + i + "]");
		}
		return replicas;
	}

	public static Replica createReplicaWithRandom(int id, String name) {
		ReplicaInfo rp = new ReplicaInfo(id);
		AsymmetricKeypair kp = Crypto.getSignatureFunction(ClassicAlgorithm.ED25519).generateKeypair();
		rp.setKey(kp);
		rp.setName(name);

		return rp;
	}

	/**
	 * 创建多端口的地址清单；
	 * 
	 * @param host      主机地址；
	 * @param n         总数量；
	 * @param portStart 起始端口号；
	 * @param portStep  每个地址的端口号递增值；
	 * @return
	 */
	public static NetworkAddress[] createMultiPortsAddresses(String host, int n, int portStart, int portStep) {
		NetworkAddress[] addrs = new NetworkAddress[n];
		for (int i = 0; i < addrs.length; i++) {
			addrs[i] = new NetworkAddress(host, portStart + portStep * i);
		}
		return addrs;
	}

	/**
	 * 将共识设置中的节点网址替换指定的网址清单中对应的值；
	 * 
	 * <p>
	 * 
	 * 按照节点顺序与网址列表顺序一一对应。
	 * 
	 * @param csSettings
	 * @param networkAddresses
	 * @return
	 */
	private static BftsmartConsensusSettings stubNetworkAddress(BftsmartConsensusSettings csSettings,
			NetworkAddress[] networkAddresses) {
		BftsmartConsensusSettings csSettingStub = Mockito.spy(csSettings);

		BftsmartNodeSettings[] nodeSettings = (BftsmartNodeSettings[]) csSettingStub.getNodes();
		nodeSettings = stubNetworkAddresses(nodeSettings, networkAddresses);

		Mockito.stub(csSettingStub.getNodes()).toReturn(nodeSettings);

		return csSettingStub;
	}

	/**
	 * 将节点配置的地址替换为网址清单中对应的值；
	 * <p>
	 * 
	 * 按照节点顺序与网址列表顺序一一对应。
	 * 
	 * @param nodeSettings
	 * @param networkAddresses
	 * @return
	 */
	private static BftsmartNodeSettings[] stubNetworkAddresses(BftsmartNodeSettings[] nodeSettings,
			NetworkAddress[] networkAddresses) {
		assert nodeSettings.length == networkAddresses.length;

		BftsmartNodeSettings[] nodeSettingStubs = new BftsmartNodeSettings[nodeSettings.length];
		for (int i = 0; i < nodeSettingStubs.length; i++) {
			nodeSettingStubs[i] = Mockito.spy(nodeSettings[i]);

			Mockito.stub(nodeSettingStubs[i].getNetworkAddress()).toReturn(networkAddresses[i]);
		}
		return nodeSettingStubs;
	}

	public static BftsmartConsensusSettings buildConsensusSettings_BFTSMaRT(String configFile, Replica[] replicas)
			throws IOException {
		Properties csProperties = PropertiesUtils.loadProperties(configFile, "UTF-8");
		return (BftsmartConsensusSettings) buildConsensusSettings(csProperties, replicas,
				BftsmartConsensusProvider.INSTANCE);
	}

	public static BftsmartConsensusSettings buildConsensusSettings_BFTSMaRT(Properties csProperties,
			Replica[] replicas) {
		return (BftsmartConsensusSettings) buildConsensusSettings(csProperties, replicas,
				BftsmartConsensusProvider.INSTANCE);
	}

	private static ConsensusViewSettings buildConsensusSettings(Properties csProperties, Replica[] replicas,
			ConsensusProvider consensusProvider) {
		return consensusProvider.getSettingsFactory().getConsensusSettingsBuilder().createSettings(csProperties,
				replicas);
	}

	private static NodeServer createNodeServer(String realmName, ConsensusViewSettings viewSettings, Replica replica,
			MessageHandlerDelegater messageHandler, StateMachineReplicate smr, ConsensusProvider consensusProvider) {
		ServerSettings serverSettings = consensusProvider.getServerFactory().buildServerSettings(realmName, viewSettings,
				replica.getAddress().toBase58());
		return consensusProvider.getServerFactory().setupServer(serverSettings, messageHandler, smr);
	}

}
