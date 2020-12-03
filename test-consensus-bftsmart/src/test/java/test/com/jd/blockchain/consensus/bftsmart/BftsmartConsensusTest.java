package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.Replica;
import com.jd.blockchain.consensus.service.NodeServer;
import com.jd.blockchain.utils.codec.Base58Utils;
import com.jd.blockchain.utils.net.NetworkAddress;
import com.jd.blockchain.utils.security.RandomUtils;

/**
 * 消息共识测试；
 * 
 * @author huanghaiquan
 *
 */
public class BftsmartConsensusTest {

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
	public void testMessageConsensus() throws IOException, InterruptedException, ConsensusSecurityException {
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
		messageSendTest.setTotalClients(2);
		messageSendTest.setMessageCountPerClient(2);
		
		messageSendTest.setMessageConsenusMillis(3000);

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

		// 退出测试前关闭；
		csEnv.closeAllClients();
		csEnv.stopNodeServers();
	}

	/**
	 * 测试在建立共识网络之后，可以动态增加节点，并且新节点也能正常参与共识；
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ConsensusSecurityException
	 */
	@Test
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

		// 执行消息一致性测试；
		messageSendTest.setTotalClients(15);
		messageSendTest.setMessageCountPerClient(4);
		messageSendTest.run(csEnv);

		// 再次新增共识节点；
		System.out.println("--------- join new replica ----------");
		NetworkAddress newNetworkAddress2 = new NetworkAddress(host, 12800, false);
		Replica newReplica2 = ConsensusEnvironment.createReplicaWithRandom(5, "节点[5]");
		csEnv.joinReplica(newReplica2, newNetworkAddress2);

		// 验证共识节点的数量；
		assertEquals(6, csEnv.getReplicaCount());

		// 执行消息一致性测试；
		messageSendTest.setMessageCountPerClient(4);
		messageSendTest.run(csEnv);

		csEnv.closeAllClients();
		csEnv.stopNodeServers();
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
//	@Test
	public void testClientViewUpdating() throws IOException, InterruptedException, ConsensusSecurityException {
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
		
		messageSendTest.setResetupClients(true);
		messageSendTest.setRequireAllClientConnected(true);
		messageSendTest.setTotalClients(2);
		messageSendTest.setMessageCountPerClient(2);
		
		messageSendTest.setMessageConsenusMillis(3000);

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

		// 退出测试前关闭；
		csEnv.closeAllClients();
		csEnv.stopNodeServers();
	}

}
