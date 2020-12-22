package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;

import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.Replica;
import com.jd.blockchain.consensus.bftsmart.service.BftsmartNodeState;
import com.jd.blockchain.utils.codec.Base58Utils;
import com.jd.blockchain.utils.concurrent.AsyncFuture;
import com.jd.blockchain.utils.net.NetworkAddress;
import com.jd.blockchain.utils.security.RandomUtils;
import com.jd.blockchain.utils.serialize.json.JSONSerializeUtils;

/**
 * 消息共识测试；
 * 
 * @author huanghaiquan
 *
 */
public class BftsmartConsensusTest {

	@Test
	public void testMessageConsensus() throws IOException, InterruptedException, ConsensusSecurityException {
		Configurator.setLevel("bftsmart", Level.ERROR);

		final int N = 4;
		final String realmName = Base58Utils.encode(RandomUtils.generateRandomBytes(32));

		NetworkAddress[] nodesNetworkAddresses = ConsensusEnvironment.createMultiPortsAddresses("127.0.0.1", N, 10600,
				10);

		ConsensusEnvironment csEnv = ConsensusEnvironment.setup_BFTSMaRT(realmName,
				"classpath:bftsmart-consensus-test-normal.config", nodesNetworkAddresses);

		// 配置用例；
		MessageConsensusTestcase messageSendTest = new MessageConsensusTestcase();
		messageSendTest.setReinstallAllNodesBeforeRunning(false);
		messageSendTest.setRestartAllNodesBeforeRunning(false);
		messageSendTest.setRequireAllNodesRunning(false);

		messageSendTest.setResetupClients(false);
		messageSendTest.setRequireAllClientConnected(true);
		messageSendTest.setTotalClients(10);
		messageSendTest.setMessageCountPerClient(2);

		messageSendTest.setMessageConsenusMillis(5000);

		// 启动 4 个共识节点；
		csEnv.startNodeServers();

		// 执行 4 个共识节点的消息消息共识一致性测试；
		messageSendTest.run(csEnv);
	}

	/**
	 * 标准测试用例：建立4个副本节点的共识网络，可以正常地达成进行共识；
	 * <p>
	 * 1. 建立 4 个副本节点的共识网络，启动全部的节点；<br>
	 * 2. 建立不少于 1 个共识客户端连接到共识网络；<br>
	 * 3. 共识客户端并发地提交共识消息，每个副本节点都能得到一致的消息队列；<br>
	 * 4. 副本节点对每一条消息都返回相同的响应，共识客户端能够得到正确的回复结果；<br>
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ConsensusSecurityException
	 */
	@Test
	public void testMessageConsensusWithNodeRestart()
			throws IOException, InterruptedException, ConsensusSecurityException {
		final int N = 4;
		final String realmName = Base58Utils.encode(RandomUtils.generateRandomBytes(32));

		NetworkAddress[] nodesNetworkAddresses = ConsensusEnvironment.createMultiPortsAddresses("127.0.0.1", N, 11600,
				10);

		ConsensusEnvironment csEnv = ConsensusEnvironment.setup_BFTSMaRT(realmName,
				"classpath:bftsmart-consensus-test-normal.config", nodesNetworkAddresses);

		// 配置用例；
		MessageConsensusTestcase messageSendTest = new MessageConsensusTestcase();
		messageSendTest.setReinstallAllNodesBeforeRunning(false);
		messageSendTest.setRestartAllNodesBeforeRunning(false);
		messageSendTest.setRequireAllNodesRunning(false);

		messageSendTest.setResetupClients(false);
		messageSendTest.setRequireAllClientConnected(true);
		messageSendTest.setTotalClients(10);
		messageSendTest.setMessageCountPerClient(2);

		messageSendTest.setMessageConsenusMillis(5000);

		// 启动 4 个共识节点；
		csEnv.startNodeServers();

		// 执行 4 个共识节点的消息消息共识一致性测试；
		messageSendTest.run(csEnv);

		// 停止 1 个共识节点后验证剩余 3 个节点的共识一致性；
		ReplicaNodeServer[] nodes = csEnv.getNodes();

		nodes[3].getNodeServer().stop();
		Thread.sleep(1000);

		messageSendTest.run(csEnv);

		// 重启节点之后；
		nodes[3] = csEnv.reinstallNodeServer(nodes[3].getReplica().getId());
		nodes[3].getNodeServer().start();
		Thread.sleep(1000);
		messageSendTest.run(csEnv);

		nodes = csEnv.getNodes();
		printNodeStates(nodes);

		// 退出测试前关闭；
		csEnv.closeAllClients();
		csEnv.stopNodeServers();
	}

	private void printNodeStates(ReplicaNodeServer[] nodes) {
		StringBuilder info = new StringBuilder();
		info.append("\r\n======================== Node State =====================\r\n");
		for (int i = 0; i < nodes.length; i++) {
			info.append("\r\n\r\n------- [" + i + "] --------\r\n");
			String stateInfo = JSONSerializeUtils.serializeToJSON(nodes[i].getNodeServer().getState(), true);
			info.append(stateInfo);
		}

		System.out.println(info.toString());
	}

	/**
	 * 验证共识服务端节点的 ID 和客户端的 ID 出现冲突时引发消息共识失败的情形；
	 * 
	 * @throws IOException
	 */
	@Test
	public void testProcessIDConflict() throws IOException {
		// 目前的 ProcessID 分配方式下，预期从 ID 为 0 的共识服务端节点认证客户端时会出现 ID 冲突；
		final int N = 4;
		final String realmName = Base58Utils.encode(RandomUtils.generateRandomBytes(32));

		NetworkAddress[] nodesNetworkAddresses = ConsensusEnvironment.createMultiPortsAddresses("127.0.0.1", N, 11200,
				10);

		ConsensusEnvironment csEnv = ConsensusEnvironment.setup_BFTSMaRT(realmName,
				"classpath:bftsmart-consensus-test-normal.config", nodesNetworkAddresses);
		// 启动 4 个共识节点；
		csEnv.startNodeServers();

		// 配置用例；
		MessageConsensusTestcase messageSendTest = new MessageConsensusTestcase();
		messageSendTest.setReinstallAllNodesBeforeRunning(false);
		messageSendTest.setRestartAllNodesBeforeRunning(false);
		messageSendTest.setRequireAllNodesRunning(false);

		messageSendTest.setResetupClients(false);
		messageSendTest.setRequireAllClientConnected(true);
		messageSendTest.setTotalClients(4);
		messageSendTest.setMessageCountPerClient(10);

		messageSendTest.setAuthenticationNodeIDs(0);

		// 执行 4 个共识节点的消息消息共识一致性测试；
		messageSendTest.setConcurrentSending(true);
		messageSendTest.setMessageConsenusMillis(10000);
		messageSendTest.run(csEnv);

	}

	/**
	 * 测试在建立共识网络之后，可以动态增加节点，并且新节点也能正常参与共识；
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ConsensusSecurityException
	 */
//	@Test
	public void testAddNodeAndConsensus() throws IOException, InterruptedException, ConsensusSecurityException {
		System.out.println("--------- BftsmartConsensusTest.testAddNodeAndConsensus----------");
		// 新建 4 个节点的共识网络；
		int N = 4;
		String realmName = Base58Utils.encode(RandomUtils.generateRandomBytes(32));
		String host = "127.0.0.1";
		NetworkAddress[] nodesNetworkAddresses = ConsensusEnvironment.createMultiPortsAddresses(host, N, 12600, 10);

		ConsensusEnvironment csEnv = ConsensusEnvironment.setup_BFTSMaRT(realmName,
				"classpath:bftsmart-consensus-test-normal.config", nodesNetworkAddresses);

		// 在新建的共识网络上先执行消息共识一致性测试；
		MessageConsensusTestcase messageSendTest = new MessageConsensusTestcase();
		messageSendTest.setReinstallAllNodesBeforeRunning(false);
		messageSendTest.setRestartAllNodesBeforeRunning(false);
		messageSendTest.setRequireAllNodesRunning(false);

		messageSendTest.setResetupClients(false);
		messageSendTest.setRequireAllClientConnected(true);
		messageSendTest.setTotalClients(2);
		messageSendTest.setMessageCountPerClient(2);

		messageSendTest.setMessageConsenusMillis(3000);

		// 执行消息一致性测试；
		csEnv.startNodeServers();
		messageSendTest.run(csEnv);

		// 新增共识节点；
		System.out.println("--------- join new replica ----------");
		NetworkAddress newNetworkAddress = new NetworkAddress(host, 12700, false);
		Replica newReplica = ConsensusEnvironment.createReplicaWithRandom(4, "节点[4]");
		csEnv.joinReplica(newReplica, newNetworkAddress);

		// 验证共识节点的数量；
		assertEquals(5, csEnv.getReplicaCount());
		assertEquals(5, csEnv.getRunningNodes().length);

		// 执行消息一致性测试；
		messageSendTest.setTotalClients(10);
		messageSendTest.setMessageCountPerClient(4);
		messageSendTest.setMessageConsenusMillis(30000);
		messageSendTest.setConcurrentSending(false);
		messageSendTest.run(csEnv);

		// 再次新增共识节点；
		System.out.println("--------- join new replica ----------");
		NetworkAddress newNetworkAddress2 = new NetworkAddress(host, 12800, false);
		Replica newReplica2 = ConsensusEnvironment.createReplicaWithRandom(5, "节点[5]");
		csEnv.joinReplica(newReplica2, newNetworkAddress2);

		// 验证共识节点的数量；
		assertEquals(6, csEnv.getReplicaCount());
		assertEquals(6, csEnv.getRunningNodes().length);

		// 执行消息一致性测试；
		messageSendTest.run(csEnv);

		csEnv.closeAllClients();
		csEnv.stopNodeServers();
	}

	/**
	 * 测试不同数量规模的节点下，领导者选举切换的正确性；
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testLeaderChange() throws IOException, InterruptedException {
		Configurator.setLevel("bftsmart.communication.server.ServerConnection", Level.OFF);
		Configurator.setLevel("bftsmart.tom.server.defaultservices.DefaultRecoverable", Level.OFF);
		Configurator.setLevel("bftsmart.tom.leaderchange.LCManager", Level.DEBUG);
		Configurator.setLevel("bftsmart.tom.leaderchange.LeaderConfirmationTask", Level.DEBUG);

		Configurator.setLevel("test.com.jd.blockchain.consensus.bftsmart.ConsensusEnvironment.NodeServerProxy",
				Level.DEBUG);

		final int N = 4;
		final String realmName = Base58Utils.encode(RandomUtils.generateRandomBytes(32));

		NetworkAddress[] nodesNetworkAddresses = ConsensusEnvironment.createMultiPortsAddresses("127.0.0.1", N, 13000,
				10);

		ConsensusEnvironment csEnv = ConsensusEnvironment.setup_BFTSMaRT(realmName,
				"classpath:bftsmart-consensus-test-normal.config", nodesNetworkAddresses);

		csEnv.startNodeServers();
		Thread.sleep(3000);

		// 配置节点状态测试用例；
		NodeStateTestcase stateTest = NodeStateTestcase.createNormalConsistantTest();
		stateTest.run(csEnv);

		// 配置消息共识测试用例；
		MessageConsensusTestcase messageSendTest = MessageConsensusTestcase.createManualResetTest(2, 2, 3000L);
		// 执行 4 个共识节点的消息消息共识一致性测试；
		messageSendTest.run(csEnv);

		ReplicaNodeServer[] nodes = csEnv.getNodes();
		assertEquals(4, nodes.length);

		// 验证所有节点的 regencyId 升级为 1， leader 都切换为 1 ；
		for (int i = 0; i < nodes.length; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(0, state.getConsensusState().getLeaderID());
			assertEquals(0, state.getLeaderState().getLeaderID());
			assertEquals(0, state.getLeaderState().getLastRegency());
			assertEquals(0, state.getLeaderState().getNextRegency());
		}

		// 把节点 0 停止，预计将进行领导者选举；选择出节点 1 为新的领导者；
		assertEquals(0, nodes[0].getReplica().getId());
		nodes[0].getNodeServer().stop();
		Thread.sleep(3000);
		// 验证剩余的所有节点[1, 2, 3]的 regencyId 升级为 1， leader 都切换为 1 ；
		for (int i = 1; i < nodes.length; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(1, state.getConsensusState().getLeaderID());
			assertEquals(1, state.getLeaderState().getLeaderID());
			assertEquals(1, state.getLeaderState().getLastRegency());
			assertEquals(1, state.getLeaderState().getNextRegency());
		}

		// 验证在 3 个节点的情况下进行共识；
		messageSendTest.run(csEnv);

		csEnv.getNode(0).getNodeServer().start();
		Thread.sleep(3000);

		// 验证节点 0 重启后经过和其它节点同步最新的 regency ；
		// 最终所有节点[0, 1, 2, 3]的 regencyId 升级为 1， leader 都切换为 1 ；
		for (int i = 0; i < nodes.length; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(1, state.getConsensusState().getLeaderID());
			assertEquals(1, state.getLeaderState().getLeaderID());
			assertEquals(1, state.getLeaderState().getLastRegency());
			assertEquals(1, state.getLeaderState().getNextRegency());
		}

		// 停止0, 1，然后重启；
		nodes[0].getNodeServer().stop();
		// 党羽当前节点为 1 ，预计领导者的停止会主动触发选举；
		// 按照当前算法，退出的领导者仍然参与几票，因此剩余 2 个节点仍然会选举出新的领导者 2；
		nodes[1].getNodeServer().stop();
		Thread.sleep(3000);
		// 验证余下的 2 个节点[2, 3]新的执政期为 2 ，新的领导者为 2；
		for (int i = 2; i < nodes.length; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(2, state.getConsensusState().getLeaderID());
			assertEquals(2, state.getLeaderState().getLeaderID());
			assertEquals(2, state.getLeaderState().getLastRegency());
			assertEquals(2, state.getLeaderState().getNextRegency());
		}

		System.out.println("\r\n\r\n-------- restart node 1 ---------\r\n");

		// 重新启动节点 1 ，验证是否能够恢复正常；
		nodes[1].getNodeServer().start();
		Thread.sleep(5000);
		// 验证余下的 3 个节点[1, 2, 3]仍然保持执政期为 2 ，领导者为 2；
		for (int i = 1; i < nodes.length; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(2, state.getConsensusState().getLeaderID());
			assertEquals(2, state.getLeaderState().getLeaderID());
			assertEquals(2, state.getLeaderState().getLastRegency());
			assertEquals(2, state.getLeaderState().getNextRegency());
		}
	}

	@Test
	public void testLeaderChange_with_5_nodes() throws IOException, InterruptedException {
		Configurator.setLevel("bftsmart.communication.server.ServerConnection", Level.OFF);
		Configurator.setLevel("bftsmart.tom.server.defaultservices.DefaultRecoverable", Level.OFF);
		Configurator.setLevel("bftsmart.tom.leaderchange.LCManager", Level.DEBUG);
		Configurator.setLevel("bftsmart.tom.leaderchange.LeaderConfirmationTask", Level.DEBUG);

		Configurator.setLevel("test.com.jd.blockchain.consensus.bftsmart.ConsensusEnvironment.NodeServerProxy",
				Level.DEBUG);

		final int N = 5;
		final String realmName = Base58Utils.encode(RandomUtils.generateRandomBytes(32));

		NetworkAddress[] nodesNetworkAddresses = ConsensusEnvironment.createMultiPortsAddresses("127.0.0.1", N, 13000,
				10);

		ConsensusEnvironment csEnv = ConsensusEnvironment.setup_BFTSMaRT(realmName,
				"classpath:bftsmart-consensus-test-normal.config", nodesNetworkAddresses);

		ReplicaNodeServer[] nodes = csEnv.getNodes();
		assertEquals(N, nodes.length);

		// 启动节点 0 - 3 ，节点 4 未启动；
		AsyncFuture<?>[] futures = new AsyncFuture[N];
		for (int i = 0; i < N-1; i++) {
			futures[i] = nodes[i].getNodeServer().start();
		}
		for (int i = 0; i < N-1; i++) {
			futures[i].get();
		}
		
		// 验证全部节点[0,1,2,3]执政期为 0 ，领导者为 0；
		for (int i = 0; i < N-1; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(0, state.getConsensusState().getLeaderID());
			assertEquals(0, state.getLeaderState().getLeaderID());
			assertEquals(0, state.getLeaderState().getLastRegency());
			assertEquals(0, state.getLeaderState().getNextRegency());
		}

		futures[N-1] = nodes[N - 1].getNodeServer().start();
		Thread.sleep(5000);

		// 验证全部节点[0,1,2,3,4]执政期为 0 ，领导者为 0；
		for (int i = 0; i < nodes.length; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(0, state.getConsensusState().getLeaderID());
			assertEquals(0, state.getLeaderState().getLeaderID());
			assertEquals(0, state.getLeaderState().getLastRegency());
			assertEquals(0, state.getLeaderState().getNextRegency());
		}

		// 停止节点 0；
		nodes[0].getNodeServer().stop();
		Thread.sleep(5000);

		// 验证节点[1,2,3,4]执政期为 1 ，领导者为 1；
		for (int i = 1; i < nodes.length; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(1, state.getConsensusState().getLeaderID());
			assertEquals(1, state.getLeaderState().getLeaderID());
			assertEquals(1, state.getLeaderState().getLastRegency());
			assertEquals(1, state.getLeaderState().getNextRegency());
		}

		// 等待 5 秒，再次验证；
		Thread.sleep(5000);
		// 验证节点[1,2,3,4]执政期为 1 ，领导者为 1；
		for (int i = 1; i < nodes.length; i++) {
			BftsmartNodeState state = (BftsmartNodeState) nodes[i].getNodeServer().getState();
			assertEquals(1, state.getConsensusState().getLeaderID());
			assertEquals(1, state.getLeaderState().getLeaderID());
			assertEquals(1, state.getLeaderState().getLastRegency());
			assertEquals(1, state.getLeaderState().getNextRegency());
		}
		
		// 配置节点状态测试用例；
		NodeStateTestcase stateTest = NodeStateTestcase.createNormalConsistantTest();
		stateTest.run(csEnv);

		// 配置消息共识测试用例；
		MessageConsensusTestcase messageSendTest = MessageConsensusTestcase.createManualResetTest(2, 2, 3000L);
		// 执行 4 个共识节点的消息消息共识一致性测试；
		messageSendTest.run(csEnv);
	}

}
