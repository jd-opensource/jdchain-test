package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.client.ConsensusClient;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.StateSnapshot;
import com.jd.blockchain.utils.SkippingIterator;
import com.jd.blockchain.utils.security.RandomUtils;

/**
 * 消息共识的测试；
 * <p>
 * 
 * 验证在一个共识网络中，多客户发送消息到共识网络，在等待一定时长后，验证每一个共识节点收到的消息是否一致，以及是否包含了全部的消息；
 * 
 * @author huanghaiquan
 *
 */
public class MessageConsensusTestcase implements ConsensusTestcase {

	private ExecutorService messageSendExecutor = Executors.newCachedThreadPool();

	private boolean reinstallPeersBeforeRunning = false;

	private boolean restartPeersBeforeRunning = false;

	private long messageConsenusMillis;

	private boolean cleanClientsAfterRunning = true;

	private boolean stopPeersAfterRunning = false;

	/**
	 * 消息大小；单位：字节；
	 */
	private int messageSize = 20;

	/**
	 * 每客户端发送的消息数量；
	 */
	private int messageCountPerClient = 10;

	/**
	 * 是否重连客户端；
	 */
	private boolean reconectClients = true;

	private int clientCount = 2;

	public void setClientCount(int clientCount) {
		this.clientCount = clientCount;
	}

	public int getClientCount() {
		return clientCount;
	}

	/**
	 * 每个消息的字节大小；
	 * 
	 * @return
	 */
	public int getMessageSize() {
		return messageSize;
	}

	public void setMessageSize(int messageSize) {
		this.messageSize = messageSize;
	}

	public int getMessageCountPerClient() {
		return messageCountPerClient;
	}

	public void setMessageCountPerClient(int messageCountPerClient) {
		this.messageCountPerClient = messageCountPerClient;
	}

	public boolean isReconectClients() {
		return reconectClients;
	}

	public void setReconectClients(boolean reconectClients) {
		this.reconectClients = reconectClients;
	}

	/**
	 * 是否在执行测试前重启全部共识节点；
	 * 
	 * <p>
	 * 默认为 true；
	 * 
	 * @return
	 */
	public boolean isRestartPeersBeforeRunning() {
		return restartPeersBeforeRunning;
	}

	public void setRestartPeersBeforeRunning(boolean restartPeersBeforeRunning) {
		this.restartPeersBeforeRunning = restartPeersBeforeRunning;
	}

	/**
	 * 是否在测试用例运行之后关闭共识节点；
	 * <p>
	 * 
	 * 默认为 false；
	 * 
	 * @return
	 */
	public boolean isStopPeersAfterRunning() {
		return stopPeersAfterRunning;
	}

	/**
	 * 是否在测试用例运行之后关闭共识节点；
	 * 
	 * @param stopPeersAfterRunning
	 */
	public void setStopPeersAfterRunning(boolean stopPeersAfterRunning) {
		this.stopPeersAfterRunning = stopPeersAfterRunning;
	}

	/**
	 * 是否重装节点；
	 * <p>
	 * 
	 * 默认为 false；
	 * 
	 * @return
	 */
	public boolean isReinstallPeersBeforeRunning() {
		return reinstallPeersBeforeRunning;
	}

	public void setReinstallPeersBeforeRunning(boolean reinstallPeersBeforeRunning) {
		this.reinstallPeersBeforeRunning = reinstallPeersBeforeRunning;
	}

	/**
	 * 执行完成测试之后是否清理客户端；
	 * <p>
	 * 
	 * 默认为 true；
	 * 
	 * @return
	 */
	public boolean isCleanClientsAfterRunning() {
		return cleanClientsAfterRunning;
	}

	public void setCleanClientsAfterRunning(boolean cleanClientsAfterRunning) {
		this.cleanClientsAfterRunning = cleanClientsAfterRunning;
	}

	/**
	 * 消息共识的时长；
	 * <p>
	 * 
	 * 测试用例将在完成发送消息之后，等待由此属性表示的时长之后再开始检查共识的结果；
	 * 
	 * @return
	 */
	public long getMessageConsenusMillis() {
		return messageConsenusMillis;
	}

	/**
	 * 设置消息共识的时长；
	 * <p>
	 * 
	 * 测试用例将在完成发送消息之后，等待由此属性表示的时长之后再开始检查共识的结果；
	 * 
	 * @param messageConsenusMillis
	 */
	public void setMessageConsenusMillis(long messageConsenusMillis) {
		this.messageConsenusMillis = messageConsenusMillis;
	}

	/**
	 * 
	 */
	@Override
	public void run(ConsensusEnvironment environment) {
		try {
			MessageSnapshotHandler[] messageHandlers = prepareMessageSnapshotHandlers(environment.getReplicaCount());

			// 处理共识节点；
			ReplicaNodeServer[] nodeServers = preparePeers(environment, messageHandlers);

			// 处理共识客户端；
			ConsensusClient[] clients = prepareClients(environment);

			try {
				// 验证环境；
				verifyEnvironmentBeforeRunning(clients, nodeServers, environment);

				// 发送消息；
				List<byte[]> sendedMessages = sendMessages(clients, environment);

				// 等待共识完成；
				waitForConsensus(environment);

				// 验证消息共识的一致性；
				verifyMessageConsistant(sendedMessages, environment, messageHandlers);
			} finally {
				if (cleanClientsAfterRunning) {
					environment.closeAllClients();
				}
			}
		} finally {
			environment.clearMessageHandlers();
			// 停止环境；
			if (stopPeersAfterRunning) {
				environment.stopNodeServers();
			}
		}
	}

	/**
	 * 在完成客户端消息发送之后，验证消息共识的一致性；
	 * 
	 * @param clientSendedMessages
	 * @param environment
	 */
	protected void verifyMessageConsistant(List<byte[]> clientSendedMessages, ConsensusEnvironment environment,
			MessageSnapshotHandler[] messageHandlers) {
		int messageCount = clientSendedMessages.size();

		// 验证每一个消息处理器处理的消息数量是否与总的发送消息数量一致；
		for (MessageHandle messageHandle : messageHandlers) {
			verify(messageHandle, times(messageCount)).processOrdered(anyInt(), any(), any());
		}

		// 验证总的消息快照是否一致；
		StateSnapshot snapshot = messageHandlers[0].getLastSnapshot();

		for (int i = 1; i < messageHandlers.length; i++) {
			StateSnapshot snapshot2 = messageHandlers[i].getLastSnapshot();
			assertEquals(snapshot.getId(), snapshot2.getId());
			assertArrayEquals(snapshot.getSnapshot(), snapshot2.getSnapshot());
		}
	}

	/**
	 * 在发送完消息之后等待共识；
	 * 
	 * <p>
	 * 
	 * 默认等待 10 秒；
	 */
	protected void waitForConsensus(ConsensusEnvironment environment) {
		try {
			Thread.sleep(getMessageConsenusMillis());
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"Thread has been iterrupted while waiting consensus finish after message sending!");
		}
	}

	/**
	 * 发送消息；
	 * 
	 * @param clients
	 */
	protected List<byte[]> sendMessages(ConsensusClient[] clients, ConsensusEnvironment environment) {
		List<byte[]> sendedMessages = Collections.synchronizedList(new LinkedList<byte[]>());

		CountDownLatch sendCompletedLatch = new CountDownLatch(clients.length);

		for (int i = 0; i < clients.length; i++) {
			ConsensusClient client = clients[i];
			messageSendExecutor.execute(new Runnable() {
				@Override
				public void run() {
					byte[][] messages = prepareClientMessages(messageCountPerClient, messageSize);

					sendMessage(client, messages);

					sendedMessages.addAll(Arrays.asList(messages));

					sendCompletedLatch.countDown();
				}
			});
		}

		try {
			sendCompletedLatch.await(30000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
		return sendedMessages;
	}

	private void sendMessage(ConsensusClient client, byte[][] messages) {
		for (int i = 0; i < messages.length; i++) {
			client.getMessageService().sendOrdered(messages[i]);
		}
	}

	protected byte[][] prepareClientMessages(int messageCount, int messageSize) {
		byte[][] messages = new byte[messageCount][];
		for (int i = 0; i < messages.length; i++) {
			messages[i] = RandomUtils.generateRandomBytes(messageSize);
		}
		return messages;
	}

	/**
	 * 验证环境是否正常；
	 * 
	 * 实现者应该实现此方法，验证环境的有效性；
	 * 
	 * @param clients
	 * @param nodeServers
	 * @param environment
	 */
	protected void verifyEnvironmentBeforeRunning(ConsensusClient[] clients, ReplicaNodeServer[] nodeServers,
			ConsensusEnvironment environment) {
	}

	protected ConsensusClient[] prepareClients(ConsensusEnvironment environment) {
		if (reconectClients && clientCount > 0) {
			try {
				return environment.resetupClients(clientCount);
			} catch (ConsensusSecurityException e) {
				throw new IllegalStateException("Error occurred while reseting clients" + e.getMessage(), e);
			}
		}

		return environment.getClients();
	}

	protected MessageSnapshotHandler[] prepareMessageSnapshotHandlers(int replicaCount) {
		MessageSnapshotHandler[] handlers = new MessageSnapshotHandler[replicaCount];
		for (int i = 0; i < handlers.length; i++) {
			MessageSnapshotHandler handler = new MessageSnapshotHandler();
			handlers[i] = handler;
		}
		handlers = spyMessageHandlers(handlers);

		return handlers;

	}

	protected ReplicaNodeServer[] preparePeers(ConsensusEnvironment environment, MessageSnapshotHandler[] handlers) {
		if (isReinstallPeersBeforeRunning()) {
			// 重装节点；
			environment.stopNodeServers();
			environment.reinstallNodeServers();
		}

		if (isRestartPeersBeforeRunning()) {
			// 重启节点；
			environment.stopNodeServers();
		}

		environment.delegateMessageHandlers(handlers);

		if (!environment.isRunning()) {
			environment.startNodeServers();
		}

		SkippingIterator<ReplicaNodeServer> nodes = environment.getNodes();
		ReplicaNodeServer[] servers = new ReplicaNodeServer[(int) nodes.getTotalCount()];
		nodes.next(servers);
		return servers;
	}

	protected MessageSnapshotHandler[] spyMessageHandlers(MessageSnapshotHandler[] handlers) {
		MessageSnapshotHandler[] spyHandlers = new MessageSnapshotHandler[handlers.length];
		for (int i = 0; i < spyHandlers.length; i++) {
			spyHandlers[i] = spy(handlers[i]);
			;
		}
		return spyHandlers;
	}

}
