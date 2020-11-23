package test.com.jd.blockchain.consensus.bftsmart;

import java.io.IOException;

import org.junit.Test;
import org.mockito.Mockito;

import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.utils.ConsoleUtils;
import com.jd.blockchain.utils.codec.Base58Utils;
import com.jd.blockchain.utils.net.NetworkAddress;
import com.jd.blockchain.utils.security.RandomUtils;

public class BftsmartConsensusTest {

	/**
	 * 标准功能用例：建立4个副本节点的共识网络，可以正常地达成进行共识；
	 * <p>
	 * 1. 建立 4 个副本节点的共识网络，启动全部的节点；<br>
	 * 2. 建立不少于 1 个共识客户端连接到共识网络；<br>
	 * 3. 共识客户端并发地提交共识消息，每个副本节点都能得到一致的消息队列；<br>
	 * 4. 副本节点对每一条消息都返回相同的响应，共识客户端能够得到正确的回复结果；<br>
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	/**
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ConsensusSecurityException
	 */
	@Test
	public void testNormal() throws IOException, InterruptedException, ConsensusSecurityException {
		final int N = 4;
		final String realmName = Base58Utils.encode(RandomUtils.generateRandomBytes(32));

		NetworkAddress[] nodesNetworkAddresses = ConesensusEnvironment.createMultiPortsAddresses("127.0.0.1", N, 11600,
				10);

		MessageHandle[] messageHandlers = new MessageHandle[N];
		for (int i = 0; i < messageHandlers.length; i++) {
			messageHandlers[i] = Mockito.mock(MessageHandle.class);
		}
		ConesensusEnvironment csEnv = ConesensusEnvironment.setup_BFTSMaRT(realmName, "classpath:bftsmart-consensus-test-normal.config",
				nodesNetworkAddresses, messageHandlers);

		csEnv.startNodeServers();

		ConsoleUtils.info("All nodes has startted!");

		csEnv.setupNewClients(6);

		ConsoleUtils.info("There are 6 clients has been setuped!");

		Thread.sleep(3000);

		csEnv.stopNodeServers();

		ConsoleUtils.info("All nodes has been stopped!");
	}

}
