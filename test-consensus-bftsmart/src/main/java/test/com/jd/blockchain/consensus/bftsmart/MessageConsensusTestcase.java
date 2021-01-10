package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.client.ConsensusClient;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.StateSnapshot;

import utils.SkippingIterator;
import utils.concurrent.AsyncFuture;
import utils.concurrent.AsyncHandle;
import utils.concurrent.CompletableAsyncFuture;

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

	private boolean reinstallAllNodesBeforeRunning = false;

	private boolean restartAllNodesBeforeRunning = false;

	private long messageConsenusMillis;

	private boolean stopPeersAfterRunning = false;

	private boolean requireAllNodesRunning = false;

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
	 * 是否重置客户端；
	 */
	private boolean resetupClients = true;

	/**
	 * 是否要求全部的客户端实例在测试开始前都处于连接状态；
	 * <p>
	 * 
	 * 未连接的客户端在测试中不参与消息发送；
	 */
	private boolean requireAllClientConnected = false;

	/**
	 * 客户端总数；
	 */
	private int totalClients = 2;

	/**
	 * 是否采用并发的方式发送同一个客户端的多条消息； 默认为 true；
	 */
	private boolean concurrentSending = true;

	/**
	 * 客户端等待回复结果的最大超时时长；单位：毫秒；
	 * 
	 * <p>
	 * 
	 * 默认值 10000 毫秒（10秒）；
	 */
	private long clientReponseAwaitTimeout = 10000;
	
	
	public MessageConsensusTestcase() {
	}
	
	/**
	 * 创建一个需要手动重置共识环境的测试；
	 * <p>
	 * 该测试不会自动对测试的共识环境 {@link ConsensusEnvironment} 的节点服务器进行重置、重启；
	 * 
	 * @param clientCount
	 * @param clientMessagePerRound
	 * @param consensusWaitingMilliseconds
	 * @return
	 */
	public static MessageConsensusTestcase createManualResetTest(int clientCount, int clientMessagePerRound, long consensusWaitingMilliseconds) {
		MessageConsensusTestcase messageSendTest = new MessageConsensusTestcase();
		messageSendTest.setReinstallAllNodesBeforeRunning(false);
		messageSendTest.setRestartAllNodesBeforeRunning(false);
		messageSendTest.setRequireAllNodesRunning(false);

		messageSendTest.setResetupClients(false);
		messageSendTest.setRequireAllClientConnected(true);
		messageSendTest.setTotalClients(clientCount);
		messageSendTest.setMessageCountPerClient(clientMessagePerRound);

		messageSendTest.setMessageConsenusMillis(consensusWaitingMilliseconds);
		
		return messageSendTest;
	}

	/**
	 * 是否采用并发的方式发送同一个客户端的多条消息； 默认为 true；
	 * 
	 * @return
	 */
	public boolean isConcurrentSending() {
		return concurrentSending;
	}

	/**
	 * 设置是否采用并发的方式发送同一个客户端的多条消息； 默认为 true；
	 * 
	 * @param concurrentSending
	 */
	public void setConcurrentSending(boolean concurrentSending) {
		this.concurrentSending = concurrentSending;
	}

	/**
	 * 客户端等待回复结果的最大超时时长；单位：毫秒；
	 * 
	 * <p>
	 * 
	 * 默认值 10000 毫秒（10秒）；
	 * 
	 * @return
	 */
	public long getClientReponseAwaitTimeout() {
		return clientReponseAwaitTimeout;
	}

	/**
	 * 设置客户端等待回复结果的最大超时时长；单位：毫秒；
	 * 
	 * <p>
	 * 
	 * 默认值 10000 毫秒（10秒）；
	 * 
	 * @param clientReponseAwaitTimeout
	 */
	public void setClientReponseAwaitTimeout(long clientReponseAwaitTimeout) {
		this.clientReponseAwaitTimeout = clientReponseAwaitTimeout;
	}

	/**
	 * 设置总的客户端数；
	 * <p>
	 * 
	 * 如果在测试发起之前，已连接的客户端数量不足指定的数量，则自动建立新的客户端以便维持设置的客户端总数；
	 * 
	 * @param totalClients
	 */
	public void setTotalClients(int totalClients) {
		this.totalClients = totalClients;
	}

	/**
	 * 客户端总数；
	 * <p>
	 * 
	 * 如果在测试发起之前，已连接的客户端数量不足指定的数量，则自动建立新的客户端以便维持设置的客户端总数；
	 * 
	 * @return
	 */
	public int getTotalClients() {
		return totalClients;
	}

	/**
	 * 是否要求全部的客户端实例在测试开始前都处于连接状态；
	 * <p>
	 * 
	 * 未连接的客户端在测试中不参与消息发送；
	 * 
	 * @return
	 */
	public boolean isRequireAllClientConnected() {
		return requireAllClientConnected;
	}

	/**
	 * 是否要求全部的客户端实例在测试开始前都处于连接状态；
	 * <p>
	 * 默认为 false；
	 * <p>
	 * 
	 * 未连接的客户端在测试中不参与消息发送；
	 * 
	 * @param requireAllClientConnected
	 */
	public void setRequireAllClientConnected(boolean requireAllClientConnected) {
		this.requireAllClientConnected = requireAllClientConnected;
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

	public boolean isResetupClients() {
		return resetupClients;
	}

	public void setResetupClients(boolean resetupClients) {
		this.resetupClients = resetupClients;
	}

	/**
	 * 是否在执行测试前重启全部共识节点；
	 * 
	 * <p>
	 * 默认为 true；
	 * 
	 * @return
	 */
	public boolean isRestartAllNodesBeforeRunning() {
		return restartAllNodesBeforeRunning;
	}

	public void setRestartAllNodesBeforeRunning(boolean restartAllNodesBeforeRunning) {
		this.restartAllNodesBeforeRunning = restartAllNodesBeforeRunning;
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
	public boolean isReinstallNodesBeforeRunning() {
		return reinstallAllNodesBeforeRunning;
	}

	public void setReinstallAllNodesBeforeRunning(boolean reinstallAllNodesBeforeRunning) {
		this.reinstallAllNodesBeforeRunning = reinstallAllNodesBeforeRunning;
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
	public boolean isRequireAllNodesRunning() {
		return requireAllNodesRunning;
	}

	/**
	 * 设置是否要求全部节点在测试开始前都保持运行；
	 * 
	 * @param requireAllNodesRunning
	 */
	public void setRequireAllNodesRunning(boolean requireAllNodesRunning) {
		this.requireAllNodesRunning = requireAllNodesRunning;
	}

	/**
	 * 
	 */
	@Override
	public void run(ConsensusEnvironment environment) {
		try {
			MessageSnapshotHandler[] messageHandlers = prepareMessageSnapshotHandlers(environment.getReplicaCount());

			// 处理共识节点；
			ReplicaNodeServer[] nodeServers = prepareNodes(environment, messageHandlers);

			// 过滤出运行的节点对应的消息处理器；
			messageHandlers = filterOutRunningHandlers(nodeServers, messageHandlers);

			// 处理共识客户端；
			ConsensusClient[] clients = prepareClients(environment);

			// 过滤出已连接的客户端列表；
			ConsensusClient[] connectedClients = filterOutConnectedClients(clients);
			if (connectedClients.length == 0) {
				throw new IllegalStateException("No connected clients!");
			}

			// 验证环境；
			verifyEnvironmentBeforeRunning(connectedClients, nodeServers, environment);

			// 发送消息；
			List<MessageRequest> messageRequests = sendMessages(connectedClients, environment);

			// 等待共识完成；
			waitForConsensus(messageRequests, environment);

			// 对运行中的节点的消息处理器验证消息共识的一致性；
			verifyMessageConsistant(messageRequests, environment, messageHandlers);

			// 验证消息回复；
			verifyMessageResponse(messageRequests, environment);
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
	private MessageSnapshotHandler[] filterOutRunningHandlers(ReplicaNodeServer[] nodeServers,
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
	 * 在完成客户端消息发送之后，验证共识服务端的消息共识处理一致性；
	 * 
	 * @param messageRequests
	 * @param environment
	 */
	protected void verifyMessageConsistant(List<MessageRequest> messageRequests, ConsensusEnvironment environment,
			MessageSnapshotHandler[] messageHandlers) {
		int messageCount = messageRequests.size();

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
	 * 验证消息回复；
	 * 
	 * <p>
	 * 
	 * 验证是否每一个消息都已经收到回复，以及回复数据的正确性；
	 * 
	 * @param messageRequests
	 * @param environment
	 */
	protected void verifyMessageResponse(List<MessageRequest> messageRequests, ConsensusEnvironment environment) {
		// 预期所有的请求都已经收到回复，且没有异常，且回复消息与发送消息一致；
		for (MessageRequest messageRequest : messageRequests) {
			assertTrue(
					String.format("Message request[%s] of client[%s] has not received response!",
							messageRequest.getMessageId(), messageRequest.getClient().getSettings().getClientId()),
					messageRequest.isCompleted());
			assertFalse(
					String.format("Message request[%s] of client[%s] has not received response!",
							messageRequest.getMessageId(), messageRequest.getClient().getSettings().getClientId()),
					messageRequest.isExceptionally());

			byte[] reponseBytes = messageRequest.getResponse(clientReponseAwaitTimeout);
			assertNotNull(reponseBytes);
			assertArrayEquals(
					String.format("Reponse bytes of message [%s] of client[%s] don's match the request bytes!",
							messageRequest.getMessageId(), messageRequest.getClient().getSettings().getClientId()),
					messageRequest.getMessage(), reponseBytes);
		}
	}

	/**
	 * 在发送完消息之后等待共识；
	 * 
	 * <p>
	 * 
	 * 默认等待 10 秒；
	 */
	protected void waitForConsensus(List<MessageRequest> messageRequests, ConsensusEnvironment environment) {
		AtomicInteger counter = new AtomicInteger(messageRequests.size());
		CountDownLatch responseLatch = new CountDownLatch(messageRequests.size());
		AsyncHandle<byte[]> responseCountdownHandle = new AsyncHandle<byte[]>() {
			@Override
			public void complete(byte[] returnValue, Throwable error) {
				counter.decrementAndGet();
				responseLatch.countDown();
			}
		};

		for (MessageRequest messageRequest : messageRequests) {
			messageRequest.onCompleted(responseCountdownHandle);
		}

		try {
			long timeout = getMessageConsenusMillis();
			boolean ok = responseLatch.await(timeout, TimeUnit.MILLISECONDS);
			if ((!ok) && counter.get() > 0) {
				//fail("Consensus timeout[" + timeout + "ms] ! There some requests have no reponse! ");
			}
		} catch (InterruptedException e1) {
			throw new IllegalStateException(
					"Thread has been iterrupted while waiting consensus finish after message sending!");
		}
	}

	/**
	 * 发送消息；
	 * 
	 * @param clients
	 */
	protected List<MessageRequest> sendMessages(ConsensusClient[] clients, ConsensusEnvironment environment) {
		List<MessageRequest> sendedMessages = Collections.synchronizedList(new LinkedList<MessageRequest>());

		List<MessageSender> senders = new LinkedList<MessageConsensusTestcase.MessageSender>();
		for (int i = 0; i < clients.length; i++) {
			ConsensusClient client = clients[i];
			MessageRequestLog[] requests = prepareClientMessages(client, messageCountPerClient);
			sendedMessages.addAll(Arrays.asList(requests));

			senders.add(new MessageSender(requests, concurrentSending, client, messageSendExecutor));
		}

		for (MessageSender sender : senders) {
			sender.send();
		}
		return sendedMessages;
	}

	protected MessageRequestLog[] prepareClientMessages(ConsensusClient client, int messageCount) {
		MessageRequestLog[] messages = new MessageRequestLog[messageCount];

		SecureRandom random = new SecureRandom();

		for (int i = 0; i < messages.length; i++) {
			byte[] messageBytes = new byte[messageSize];
			random.nextBytes(messageBytes);
			messages[i] = new MessageRequestLog(i, messageBytes, client);
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

	/**
	 * 过滤出处于连接状态的客户端；
	 * 
	 * @param clients
	 * @return
	 */
	private ConsensusClient[] filterOutConnectedClients(ConsensusClient[] clients) {
		List<ConsensusClient> connectedClients = new ArrayList<>();
		for (int i = 0; i < clients.length; i++) {
			if (clients[i].isConnected()) {
				connectedClients.add(clients[i]);
			}
		}
		return connectedClients.toArray(new ConsensusClient[connectedClients.size()]);
	}

	/**
	 * 准备客户端；
	 * <p>
	 * 
	 * 此方法将按照配置的方式从共识环境建立客户端连接，维持配置指定的客户端数量；
	 * <p>
	 * 
	 * @param environment 共识环境；
	 * @return 返回数量等于 {@link #getTotalClients()} 的客户端列表；受配置
	 *         {@link #requireAllClientConnected} 影响，返回的客户端不一定都处于连接状态；
	 */
	protected ConsensusClient[] prepareClients(ConsensusEnvironment environment) {
		if (totalClients <= 0) {
			throw new IllegalArgumentException("The total client is negative or zero!");
		}
		if (resetupClients) {
			environment.closeAllClients();
		}

		int[] authNodeIds = prepareAuthenticationNodeIDs(environment);

		// 维持指定的客户端数量；
		ConsensusClient[] clients = environment.getClients();
		if (clients.length < totalClients) {
			try {
				environment.setupNewClients(totalClients - clients.length, authNodeIds);
			} catch (ConsensusSecurityException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		} else if (clients.length > totalClients) {
			// 关闭超出数量的客户端，优先清理已关闭的客户端；
			int closingNumber = clients.length - totalClients;

			// 重新排序客户端列表，把已经关闭连接的客户端排列在前面；
			Arrays.sort(clients, new Comparator<ConsensusClient>() {
				@Override
				public int compare(ConsensusClient o1, ConsensusClient o2) {
					int v1 = o1.isConnected() ? 1 : 0;
					int v2 = o2.isConnected() ? 1 : 0;
					return v1 - v2;
				}
			});
			//
			for (int i = 0; i < closingNumber; i++) {
				environment.closeClient(clients[i]);
			}
		}
		clients = environment.getClients();

		// 确保客户端处于连接状态；
		if (requireAllClientConnected) {
			for (int i = 0; i < clients.length; i++) {
				if (!clients[i].isConnected()) {
					clients[i].connect();
				}
			}
		}

		return clients;
	}

	protected int[] prepareAuthenticationNodeIDs(ConsensusEnvironment environment) {
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

		return authNodeIds;
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

	protected ReplicaNodeServer[] prepareNodes(ConsensusEnvironment environment, MessageSnapshotHandler[] handlers) {
		if (isReinstallNodesBeforeRunning()) {
			// 重装节点；
			environment.stopNodeServers();
			environment.reinstallNodeServers();
		}

		if (isRestartAllNodesBeforeRunning()) {
			// 重启节点；
			environment.stopNodeServers();
			environment.startNodeServers();
		}

		environment.delegateMessageHandlers(handlers);

		if (requireAllNodesRunning && (!environment.isTotalRunning())) {
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

	private static class MessageRequestLog implements MessageRequest {

		private CompletableAsyncFuture<byte[]> retnFuture = new CompletableAsyncFuture<>();

		private byte[] messageBytes;

		private ConsensusClient client;

		private int messageId;

		private volatile boolean sended = false;

		public MessageRequestLog(int messageId, byte[] messageBytes, ConsensusClient client) {
			this.messageId = messageId;
			this.messageBytes = messageBytes;
			this.client = client;
		}

		@Override
		public byte[] getMessage() {
			return messageBytes;
		}

		@Override
		public boolean isCompleted() {
			return retnFuture.isDone();
		}

		@Override
		public boolean isExceptionally() {
			return retnFuture.isExceptionally();
		}

		@Override
		public byte[] getResponse(long timeOut) {
			return retnFuture.get(timeOut, TimeUnit.MILLISECONDS);
		}

		@Override
		public ConsensusClient getClient() {
			return client;
		}

		@Override
		public boolean isSended() {
			return retnFuture != null;
		}

		public int getMessageId() {
			return messageId;
		}

		public void execute() {
			execute(null);
		}

		public synchronized void execute(AsyncHandle<byte[]> handle) {
			if (sended) {
				throw new IllegalStateException("Request cann't send repeatedly!");
			}
			if (handle != null) {
				retnFuture.whenComplete(handle);
			}

			AsyncFuture<byte[]> future = client.getMessageService().sendOrdered(messageBytes);
			sended = true;
			future.whenComplete(new AsyncHandle<byte[]>() {
				@Override
				public void complete(byte[] returnValue, Throwable error) {
					if (error == null) {
						retnFuture.complete(returnValue);
					}else {
						retnFuture.error(error);
					}
				}
			});
		}

		public void onCompleted(AsyncHandle<byte[]> handle) {
			retnFuture.whenComplete(handle);
		}
	}

	private static class MessageSender {

		private MessageRequestLog[] messeageRequests;

		private ConsensusClient client;

		private Executor executor;

		private boolean concurrent = false;

		public MessageSender(MessageRequestLog[] messeageRequests, boolean concurrent, ConsensusClient client,
				Executor executor) {
			this.messeageRequests = messeageRequests;
			this.concurrent = concurrent;
			this.client = client;
			this.executor = executor;
		}

		public void send() {
			if (concurrent) {
				for (MessageRequestLog request : messeageRequests) {
					executor.execute(new Runnable() {
						@Override
						public void run() {
							request.execute();
						}
					});
				}
			} else {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						for (MessageRequestLog request : messeageRequests) {
							request.execute();
						}
					}
				});
			}
		}

		/**
		 * 是否并发发送；
		 * 
		 * @return
		 */
		public boolean isConcurrent() {
			return concurrent;
		}

		/**
		 * 设置是否并发发送；
		 * 
		 * @param concurrent
		 */
		public void setConcurrent(boolean concurrent) {
			this.concurrent = concurrent;
		}

	}

}
