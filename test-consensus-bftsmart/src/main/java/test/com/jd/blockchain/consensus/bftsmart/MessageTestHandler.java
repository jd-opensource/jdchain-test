package test.com.jd.blockchain.consensus.bftsmart;

import java.util.Map;

import com.jd.blockchain.consensus.service.ConsensusContext;
import com.jd.blockchain.consensus.service.ConsensusMessageContext;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.StateSnapshot;
import com.jd.blockchain.utils.concurrent.AsyncFuture;

public class MessageTestHandler implements MessageHandle {
	

	@Override
	public String beginBatch(ConsensusContext consensusContext) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AsyncFuture<byte[]> processOrdered(int messageSequence, byte[] message, ConsensusMessageContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StateSnapshot completeBatch(ConsensusMessageContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void commitBatch(ConsensusMessageContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollbackBatch(int reasonCode, ConsensusMessageContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AsyncFuture<byte[]> processUnordered(byte[] message) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StateSnapshot getStateSnapshot(ConsensusContext consensusContext) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StateSnapshot getGenesisStateSnapshot(ConsensusContext consensusContext) {
		// TODO Auto-generated method stub
		return null;
	}

}
