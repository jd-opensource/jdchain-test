package test.com.jd.blockchain.consensus.bftsmart;

import java.util.Queue;

import com.jd.blockchain.consensus.client.ConsensusClient;

public class NormalConsensusTest extends DefaultConsensusTestcase {

	@Override
	protected void verifyEnvironmentBeforeRunning(ConsensusClient[] clients, ReplicaNodeServer[] nodeServers,
			ConsensusEnvironment environment) {
	}

	@Override
	protected void verifyMessageConsensus(Queue<byte[]> clientSendedMessages, ConsensusEnvironment environment) {

		super.verifyMessageConsensus(clientSendedMessages, environment);

	}

}
