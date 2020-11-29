package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.Replica;
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

		// 执行消息消息共识一致性测试；
		MessageConsensusTestcase messageSendTest = new MessageConsensusTestcase();
		messageSendTest.setReconectClients(true);
		messageSendTest.setClientCount(2);
		messageSendTest.setMessageConsenusMillis(3000);

		messageSendTest.run(csEnv);
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
		NetworkAddress[] nodesNetworkAddresses = ConsensusEnvironment.createMultiPortsAddresses(host, N, 11600, 10);

		ConsensusEnvironment csEnv = ConsensusEnvironment.setup_BFTSMaRT(realmName,
				"classpath:bftsmart-consensus-test-normal.config", nodesNetworkAddresses);

		// 在新建的共识网络上先执行消息共识一致性测试；
		MessageConsensusTestcase messageSendTest = new MessageConsensusTestcase();
		messageSendTest.setReinstallPeersBeforeRunning(false);
		messageSendTest.setRestartPeersBeforeRunning(false);
		messageSendTest.setReconectClients(true);
		messageSendTest.setClientCount(2);
		messageSendTest.setMessageCountPerClient(2);
		messageSendTest.setCleanClientsAfterRunning(false);

		// 执行消息一致性测试；
		messageSendTest.setMessageConsenusMillis(10000);
		messageSendTest.run(csEnv);

		// 新增共识节点；
		System.out.println("--------- join new replica ----------");
		NetworkAddress newNetworkAddress = new NetworkAddress(host, 11700, false);
		Replica newReplica = ConsensusEnvironment.createReplicaWithRandom(4, "节点[4]");
		csEnv.joinReplica(newReplica, newNetworkAddress);

		// 验证共识节点的数量；
		assertEquals(5, csEnv.getReplicaCount());

		
		// 执行消息一致性测试；
		messageSendTest.run(csEnv);
	}

}
