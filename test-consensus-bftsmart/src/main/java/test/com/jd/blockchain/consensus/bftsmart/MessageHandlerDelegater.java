package test.com.jd.blockchain.consensus.bftsmart;

import java.util.Map;

import com.jd.blockchain.consensus.service.ConsensusContext;
import com.jd.blockchain.consensus.service.ConsensusMessageContext;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.StateSnapshot;

import utils.concurrent.AsyncFuture;

public class MessageHandlerDelegater implements MessageHandle {
	
//	private final NullMessageHandler NULL_HANDLER = new NullMessageHandler();

	private volatile MessageHandle messageHandle;


	public MessageHandlerDelegater() {
	}

	public MessageHandlerDelegater(MessageHandle handler) {
		this.messageHandle = messageHandle;
	}

	public void delegateTo(MessageHandle messageHandle) {
		this.messageHandle = messageHandle;
	}

	public void clear() {
		this.messageHandle = null;
	}

	private void check() {
		if (messageHandle == null) {
			throw new IllegalStateException("No message handler!");
		}
	}

	@Override
	public String beginBatch(ConsensusContext consensusContext) {
		check();
//		if (messageHandle == null) {
//			return NULL_HANDLER.beginBatch(consensusContext);
//		}
		return messageHandle.beginBatch(consensusContext);
	}

	@Override
	public AsyncFuture<byte[]> processOrdered(int messageSequence, byte[] message, ConsensusMessageContext context) {
		check();
//		if (messageHandle == null) {
//			return NULL_HANDLER.processOrdered(messageSequence, message, context);
//		}
		return messageHandle.processOrdered(messageSequence, message, context);
	}

	@Override
	public StateSnapshot completeBatch(ConsensusMessageContext context) {
		check();
//		if (messageHandle == null) {
//			return NULL_HANDLER.completeBatch(context);
//		}
		return messageHandle.completeBatch(context);
	}

	@Override
	public void commitBatch(ConsensusMessageContext context) {
		check();
//		if (messageHandle == null) {
//			NULL_HANDLER.commitBatch(context);
//			return;
//		}
		messageHandle.commitBatch(context);
	}

	@Override
	public void rollbackBatch(int reasonCode, ConsensusMessageContext context) {
		check();
//		if (messageHandle == null) {
//			NULL_HANDLER.rollbackBatch(reasonCode, context);
//			return;
//		}
		messageHandle.rollbackBatch(reasonCode, context);
	}

	@Override
	public AsyncFuture<byte[]> processUnordered(byte[] message) {
		check();
//		if (messageHandle == null) {
//			return NULL_HANDLER.processUnordered(message);
//		}
		return messageHandle.processUnordered(message);
	}

	@Override
	public StateSnapshot getLatestStateSnapshot(String realName) {
		return null;
	}

	@Override
	public StateSnapshot getGenesisStateSnapshot(String realName) {
		return null;
	}

	@Override
	public int getCommandsNumByCid(String realName, int cid) {
		return 0;
	}

	@Override
	public byte[][] getCommandsByCid(String realName, int cid, int currCidCommandsSize) {
		return new byte[0][];
	}

	@Override
	public byte[] getBlockHashByCid(String realName, int cid) {
		return new byte[0];
	}

	@Override
	public long getTimestampByCid(String realName, int cid) {
		return 0;
	}

//	@Override
//	public StateSnapshot getStateSnapshot(ConsensusContext consensusContext) {
//		check();
////		if (messageHandle == null) {
////			return NULL_HANDLER.getStateSnapshot(consensusContext);
////		}
//		return messageHandle.getStateSnapshot(consensusContext);
//	}
//
//	@Override
//	public StateSnapshot getGenesisStateSnapshot(ConsensusContext consensusContext) {
//		check();
////		if (messageHandle == null) {
////			return NULL_HANDLER.getGenesisStateSnapshot(consensusContext);
////		}
//		return messageHandle.getGenesisStateSnapshot(consensusContext);
//	}

}
