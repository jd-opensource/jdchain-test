package test.com.jd.blockchain.consensus.bftsmart;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jd.blockchain.consensus.service.ConsensusContext;
import com.jd.blockchain.consensus.service.ConsensusMessageContext;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.StateSnapshot;
import com.jd.blockchain.crypto.Crypto;
import com.jd.blockchain.crypto.HashDigester;
import com.jd.blockchain.crypto.service.classic.ClassicAlgorithm;
import com.jd.blockchain.utils.concurrent.AsyncFuture;
import com.jd.blockchain.utils.concurrent.CompletableAsyncFuture;
import com.jd.blockchain.utils.io.BytesUtils;

public class MessageTestHandler implements MessageHandle {

	private static Logger LOGGER = LoggerFactory.getLogger(MessageTestHandler.class);

	private AtomicLong batchID = new AtomicLong();

	private volatile long currentBatchID;
	private volatile ConsensusContext currentContext;

	private volatile HashDigester hashDigester;

	private volatile List<OrderedMessageHandle> orderedMessages;
	
	private volatile StateSnapshot currentSnaphot;

	/**
	 * 报告的错误；
	 */
	private volatile Throwable error;

	@Override
	public synchronized String beginBatch(ConsensusContext consensusContext) {
		if (this.currentContext != null) {
			IllegalStateException error = new IllegalStateException(
					"The current consensus has not been closed! Cann't begin another one!");
			reportError(error);
			throw error;
		}
		currentBatchID = batchID.getAndIncrement();
		this.currentContext = consensusContext;
		this.hashDigester = Crypto.getHashFunction(ClassicAlgorithm.SHA256).beginHash();
		this.hashDigester.update(BytesUtils.toBytes(consensusContext.getRealmName()));
		this.hashDigester.update(BytesUtils.toBytes(currentBatchID));
		this.orderedMessages = new LinkedList<OrderedMessageHandle>();
		return String.valueOf(currentBatchID);
	}

	@Override
	public AsyncFuture<byte[]> processOrdered(int messageSequence, byte[] messageBytes,
			ConsensusMessageContext messageContext) {
		
		this.hashDigester.update(BytesUtils.toBytes(messageSequence));
		this.hashDigester.update(BytesUtils.toBytes(messageContext.getTimestamp()));
		this.hashDigester.update(messageBytes);
		
		OrderedMessageHandle msgHandle = new OrderedMessageHandle(messageSequence, messageBytes, messageContext);
		orderedMessages.add(msgHandle);
		
		return msgHandle;
	}

	@Override
	public synchronized StateSnapshot completeBatch(ConsensusMessageContext context) {
		if (currentSnaphot != null) {
			IllegalStateException error = new IllegalStateException("There is a batch has not been committed!");
			reportError(error);
			throw error;
		}
		if (hashDigester == null) {
			IllegalStateException error = new IllegalStateException("There is no batch in processing!");
			reportError(error);
			throw error;
		}
		byte[] messagesHash = this.hashDigester.complete();
		this.currentSnaphot = new OrderedMessageSnapshot(currentBatchID, messagesHash);
		return currentSnaphot;
	}

	@Override
	public void commitBatch(ConsensusMessageContext context) {
		if (currentSnaphot == null) {
			IllegalStateException error = new IllegalStateException("There is no completed batch!");
			reportError(error);
			throw error;
		}
		this.currentBatchID = -1;
		this.currentContext = null;
		this.hashDigester  = null;
		this.orderedMessages = null;
		this.currentSnaphot = null;
	}

	@Override
	public void rollbackBatch(int reasonCode, ConsensusMessageContext context) {
		this.currentBatchID = -1;
		this.currentContext = null;
		this.hashDigester  = null;
		this.orderedMessages = null;
		this.currentSnaphot = null;
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

	private void reportError(Throwable error) {
		LOGGER.error(error.getMessage(), error);

		this.error = error;
	}

	private void reportError(Throwable error, String errorMessage, Object... args) {
		String errMsg = String.format(errorMessage, args);
		LOGGER.error(errMsg, error);

		this.error = error;
	}

	private static class OrderedMessageHandle extends CompletableAsyncFuture<byte[]> {

		private long messageSequence;

		private byte[] messageBytes;

		private ConsensusMessageContext messageContext;

		public OrderedMessageHandle(long messageSequence, byte[] messageBytes, ConsensusMessageContext messageContext) {
			this.messageSequence = messageSequence;
			this.messageBytes = messageBytes;
			this.messageContext = messageContext;
		}

	}
	
	private static class OrderedMessageSnapshot implements StateSnapshot{
		
		private long batchID;
		
		private byte[] hashSnapshot;
		
		public OrderedMessageSnapshot(long batchID, byte[] hashSnapshot) {
			this.batchID = batchID;
			this.hashSnapshot = hashSnapshot;
		}

		@Override
		public long getId() {
			return batchID;
		}

		@Override
		public byte[] getSnapshot() {
			return hashSnapshot;
		}
		
	}
}
