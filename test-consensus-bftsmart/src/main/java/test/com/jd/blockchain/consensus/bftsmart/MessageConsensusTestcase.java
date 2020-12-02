package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.client.ConsensusClient;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.StateSnapshot;
import com.jd.blockchain.utils.SkippingIterator;
import com.jd.blockchain.utils.concurrent.AsyncFuture;
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

	private static Logger LOGGER = LoggerFactory.getLogger(MessageConsensusTestcase.class);

	private ExecutorService messageSendExecutor = Executors.newCachedThreadPool();

	private boolean reinstallPeersBeforeRunning = false;

	private boolean restartPeersBeforeRunning = false;

	private long messageConsenusMillis;

	private boolean cleanClientsAfterRunning = true;

	private boolean stopPeersAfterRunning = false;

	private boolean requireTotalRunning = false;

	private int[] authenticationNodeIDs;

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
	 * 获取用于认证的节点服务器 ID 列表；
	 * <p>
	 * 
	 * 如果列表为空，则会以所有的共识节点作为客户端认证服务器节点；
	 * 
	 * @return
	 */
	public int[] getAuthenticationNodeIDs() {
		return authenticationNodeIDs;
	}

	/**
	 * 设置用于认证的节点服务器 ID 列表；
	 * <p>
	 * 
	 * 如果列表为空，则会以所有的共识节点作为客户端认证服务器节点；
	 * 
	 * @param authenticationNodeIDs
	 */
	public void setAuthenticationNodeIDs(int... authenticationNodeIDs) {
		this.authenticationNodeIDs = authenticationNodeIDs;
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
	 * 是否要求全部节点在测试开始前都保持运行；
	 * 
	 * @return
	 */
	public boolean isRequireTotalRunning() {
		return requireTotalRunning;
	}

	/**
	 * 设置是否要求全部节点在测试开始前都保持运行；
	 * 
	 * @param requireTotalRunning
	 */
	public void setRequireTotalRunning(boolean requireTotalRunning) {
		this.requireTotalRunning = requireTotalRunning;
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

			// 过滤出运行的节点对应的消息处理器；
			messageHandlers = getRunningHandlers(nodeServers, messageHandlers);

			// 处理共识客户端；
			ConsensusClient[] clients = prepareClients(environment);

			try {
				// 验证环境；
				verifyEnvironmentBeforeRunning(clients, nodeServers, environment);

				// 发送消息；
				List<AsyncFuture<byte[]>> sendedMessages = sendMessages(clients, environment);

				// 等待共识完成；
				waitForConsensus(environment);

				// 对运行中的节点的消息处理器验证消息共识的一致性；
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
	 * 返回运行中的节点对应的消息处理器；
	 * 
	 * @param nodeServers
	 * @param messageHandlers
	 * @return
	 */
	private MessageSnapshotHandler[] getRunningHandlers(ReplicaNodeServer[] nodeServers,
			MessageSnapshotHandler[] messageHandlers) {
		assert nodeServers.length == messageHandlers.length;

		List<MessageSnapshotHandler> runningHandlers = new ArrayList<MessageSnapshotHandler>();
		for (int i = 0; i < nodeServers.length; i++) {
			if (nodeServers[i].getNodeServer().isRunning()) {
				runningHandlers.add(messageHandlers[i]);
			}
		}

		return runningHandlers.toArray(new MessageSnapshotHandler[runningHandlers.size()]);
	}

	/**
	 * 在完成客户端消息发送之后，验证消息共识的一致性；
	 * 
	 * @param clientSendedMessages
	 * @param environment
	 */
	protected void verifyMessageConsistant(List<AsyncFuture<byte[]>> clientSendedMessages,
			ConsensusEnvironment environment, MessageSnapshotHandler[] messageHandlers) {
		int messageCount = clientSendedMessages.size();

		// 验证每一个消息处理器处理的消息数量是否与总的发送消息数量一致；
		LOGGER.debug("verify message consistant in all message handlers! -- handler count=" + messageHandlers.length);
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
	protected List<AsyncFuture<byte[]>> sendMessages(ConsensusClient[] clients, ConsensusEnvironment environment) {
		List<AsyncFuture<byte[]>> sendedMessages = Collections.synchronizedList(new LinkedList<AsyncFuture<byte[]>>());

		CountDownLatch sendCompletedLatch = new CountDownLatch(clients.length);

		for (int i = 0; i < clients.length; i++) {
			ConsensusClient client = clients[i];
			messageSendExecutor.execute(new Runnable() {
				@Override
				public void run() {
					byte[][] messages = prepareClientMessages(messageCountPerClient, messageSize);

					AsyncFuture<byte[]>[] futures = sendMessage(client, messages);

					sendedMessages.addAll(Arrays.asList(futures));

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

	private AsyncFuture<byte[]>[] sendMessage(ConsensusClient client, byte[][] messages) {
		@SuppressWarnings("unchecked")
		AsyncFuture<byte[]>[] futures = new AsyncFuture[messages.length];
		for (int i = 0; i < messages.length; i++) {
			futures[i] = client.getMessageService().sendOrdered(messages[i]);
		}

		return futures;
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
				int[] authNodeIds = null;
				if (authenticationNodeIDs == null) {
					ReplicaNodeServer[] runningNodes = environment.getRunningNodes();
					if (runningNodes.length == 0) {
						throw new IllegalStateException("No running nodes in the specified consensus environment!");
					}
					authNodeIds = new int[runningNodes.length];
					for (int i = 0; i < runningNodes.length; i++) {
						authNodeIds[i] = runningNodes[i].getReplica().getId();
					}
				} else {
					authNodeIds = authenticationNodeIDs.clone();
				}
				return environment.resetupClients(clientCount, authNodeIds);
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
			environment.startNodeServers();
		}

		environment.delegateMessageHandlers(handlers);

		if (requireTotalRunning && (!environment.isTotalRunning())) {
			environment.startNodeServers();
		}

		SkippingIterator<ReplicaNodeServer> nodes = environment.getNodesIterator();
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
