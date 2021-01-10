package test.com.jd.blockchain.consensus.bftsmart;

import java.util.concurrent.atomic.AtomicLong;

import com.jd.blockchain.consensus.service.ConsensusContext;
import com.jd.blockchain.consensus.service.ConsensusMessageContext;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.StateSnapshot;

import utils.BusinessException;
import utils.concurrent.AsyncFuture;
import utils.concurrent.CompletableAsyncFuture;
import utils.io.BytesUtils;


public class NullMessageHandler implements MessageHandle{
	
	private AtomicLong id = new AtomicLong(0);
	
	private EmptyStateSnapshot genesitSnapshot= new EmptyStateSnapshot(-1);
	private EmptyStateSnapshot lastSnapshot;
	
	private EmptyStateSnapshot batch;
	
	
	public NullMessageHandler() {
	}
	
	
	@Override
	public synchronized String beginBatch(ConsensusContext consensusContext) {
		if (batch != null) {
			throw new IllegalStateException("Last batch doen't complete!");
		}
		batch = new EmptyStateSnapshot(id.getAndIncrement());
		return batch.id + "";
	}

	@Override
	public AsyncFuture<byte[]> processOrdered(int messageSequence, byte[] message, ConsensusMessageContext context) {
		if (batch == null) {
			throw new IllegalStateException("No batch has began!");
		}
		return batch.emptyFuture;
	}

	@Override
	public StateSnapshot completeBatch(ConsensusMessageContext context) {
		if (batch == null) {
			throw new IllegalStateException("No batch has began!");
		}
		if (!context.getBatchId().equals(batch.id+"")) {
			throw new IllegalStateException("Batch ID of ConsensusMessageContext doesn't match!");
		}
		batch.emptyFuture.complete(BytesUtils.EMPTY_BYTES);
		return batch;
	}

	@Override
	public void commitBatch(ConsensusMessageContext context) {
		if (batch == null) {
			throw new IllegalStateException("No batch has began!");
		}
		if (!context.getBatchId().equals(batch.id+"")) {
			throw new IllegalStateException("Batch ID of ConsensusMessageContext doesn't match!");
		}
		lastSnapshot = batch;
		batch = null;
	}

	@Override
	public void rollbackBatch(int reasonCode, ConsensusMessageContext context) {
		if (batch == null) {
			throw new IllegalStateException("No batch has began!");
		}
		if (!context.getBatchId().equals(batch.id+"")) {
			throw new IllegalStateException("Batch ID of ConsensusMessageContext doesn't match!");
		}
		batch.emptyFuture.error(new BusinessException(reasonCode));
		lastSnapshot = batch;
		batch = null;
	}

	@Override
	public AsyncFuture<byte[]> processUnordered(byte[] message) {
		CompletableAsyncFuture<byte[]> emptyFuture = new CompletableAsyncFuture<>();
		emptyFuture.complete(BytesUtils.EMPTY_BYTES);
		return emptyFuture;
	}

	@Override
	public StateSnapshot getStateSnapshot(ConsensusContext consensusContext) {
		return lastSnapshot == null ? genesitSnapshot : lastSnapshot;
	}

	@Override
	public StateSnapshot getGenesisStateSnapshot(ConsensusContext consensusContext) {
		return genesitSnapshot;
	}

	
	private static class EmptyStateSnapshot implements StateSnapshot{
		
		private long id;
		
		private CompletableAsyncFuture<byte[]> emptyFuture = new CompletableAsyncFuture<>();
		
		public EmptyStateSnapshot(long id) {
			this.id = id;
		}

		@Override
		public long getId() {
			return id;
		}

		@Override
		public byte[] getSnapshot() {
			return BytesUtils.EMPTY_BYTES;
		}
		
	}
}
