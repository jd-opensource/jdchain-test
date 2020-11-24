package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.LinkedList;
import java.util.Queue;

import com.jd.blockchain.consensus.ConsensusSecurityException;
import com.jd.blockchain.consensus.client.ConsensusClient;
import com.jd.blockchain.consensus.service.MessageHandle;
import com.jd.blockchain.consensus.service.StateSnapshot;
import com.jd.blockchain.utils.SkippingIterator;
import com.jd.blockchain.utils.security.RandomUtils;

public abstract class DefaultConsensusTestcase implements ConsensusTestcase {

	private boolean restartPeersBeforeRunning = true;

	private int resetupClientCountBeforeRunning = 1;

	private long messageConsenusMillis;

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
	 * 是否在运行测试前重装全部共识节点；
	 * <p>
	 * 
	 * 返回非空列表则表示要重装；如果非空，则返回的数量必须等于指定的节点数；
	 * <p>
	 * 
	 * 默认返回 null，不重装；
	 * 
	 * @return
	 */
	protected MessageTestHandler[] resetupPeersBeforeRunning(int nodeCount) {
		MessageTestHandler[] handlers = new MessageTestHandler[nodeCount];
		for (int i = 0; i < handlers.length; i++) {
			MessageTestHandler handler = new MessageTestHandler();
			handlers[i] = handler;
		}
		return handlers;
	}

	/**
	 * 是否在运行测试前重建全部客户端；
	 * <p>
	 * 
	 * 返回值表示要建立连接的客户端的数量；
	 * <p>
	 * 
	 * 如果返回值大于 0 ，则重建相应数量的客户端；
	 * <p>
	 * 如果返回值小于等于 0，则不作重建操作；
	 * <p>
	 * 
	 * @return
	 */
	public int getResetupClientCountBeforeRunning() {
		return resetupClientCountBeforeRunning;
	}

	public void setResetupClientCountBeforeRunning(int resetupClientCountBeforeRunning) {
		this.resetupClientCountBeforeRunning = resetupClientCountBeforeRunning;
	}

	/**
	 * 
	 */
	@Override
	public void run(ConsensusEnvironment environment) {
		try {
			// 处理共识节点；
			ReplicaNodeServer[] nodeServers = preparePeers(environment);

			// 处理共识客户端；
			ConsensusClient[] clients = prepareClients(environment);

			// 验证环境；
			verifyEnvironmentBeforeRunning(clients, nodeServers, environment);

			// 发送消息；
			Queue<byte[]> sendedMessages = sendMessages(clients, environment);

			// 等待共识完成；
			waitForConsensus(environment);

			// 验证消息共识的一致性；
			verifyMessageConsensus(sendedMessages, environment);
		} finally {
			// 停止环境；
			stopEnvironmentAfterRunning(environment);
		}
	}

	private void stopEnvironmentAfterRunning(ConsensusEnvironment environment) {
		environment.closeAllClients();
		environment.stopNodeServers();
	}

	/**
	 * 在完成客户端消息发送之后，验证消息共识的一致性；
	 * 
	 * @param clientSendedMessages
	 * @param environment
	 */
	protected void verifyMessageConsensus(Queue<byte[]> clientSendedMessages, ConsensusEnvironment environment) {
		MessageHandle[] messageHandlers = environment.getMessageHandlers();

		assertTrue(messageHandlers.length > 0);
		int messageCount = clientSendedMessages.size();

		// 验证每一个消息处理器处理的消息数量是否与总的发送消息数量一致；
		for (MessageHandle messageHandle : messageHandlers) {
			verify(messageHandle, times(messageCount)).processOrdered(anyInt(), any(), any());
		}

		// 验证总的消息快照是否一致；
		StateSnapshot snapshot = ((MessageTestHandler) messageHandlers[0]).getLastSnapshot();
		for (int i = 1; i < messageHandlers.length; i++) {
			StateSnapshot snapshot2 = ((MessageTestHandler) messageHandlers[i]).getLastSnapshot();
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
	protected Queue<byte[]> sendMessages(ConsensusClient[] clients, ConsensusEnvironment environment) {
		Queue<byte[]> sendedMessages = new LinkedList<byte[]>();
		for (int i = 0; i < clients.length; i++) {
			byte[][] messages = prepareClientMessages(i, clients[i]);
			for (int j = 0; j < messages.length; j++) {
				clients[i].getMessageService().sendOrdered(messages[i]);
				sendedMessages.add(messages[i]);
			}
		}
		return sendedMessages;
	}

	protected byte[][] prepareClientMessages(int index, ConsensusClient client) {
		byte[][] messages = new byte[5][];
		int messageSize = 16;
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
		fail("Environment is not ready!");
	}

	protected ConsensusClient[] prepareClients(ConsensusEnvironment environment) {
		int restupClients = getResetupClientCountBeforeRunning();
		if (restupClients > 0) {
			try {
				return environment.resetupClients(restupClients);
			} catch (ConsensusSecurityException e) {
				throw new IllegalStateException("Error occurred while reseting clients" + e.getMessage(), e);
			}
		}

		return environment.getClients();
	}

	protected ReplicaNodeServer[] preparePeers(ConsensusEnvironment environment) {
		MessageTestHandler[] newHandles = resetupPeersBeforeRunning(environment.getReplicaCount());
		newHandles = spyMessageHandlers(newHandles);

		if (newHandles != null) {
			// 重装节点；
			environment.stopNodeServers();
			environment.setupNodeServers(newHandles);
		}

		if (isRestartPeersBeforeRunning()) {
			// 重启节点；
			environment.stopNodeServers();
		}
		if (!environment.isRunning()) {
			environment.startNodeServers();
		}

		SkippingIterator<ReplicaNodeServer> nodes = environment.getNodes();
		ReplicaNodeServer[] servers = new ReplicaNodeServer[(int) nodes.getTotalCount()];
		nodes.next(servers);
		return servers;
	}

	protected MessageTestHandler[] spyMessageHandlers(MessageTestHandler[] handlers) {
		MessageTestHandler[] spyHandlers = new MessageTestHandler[handlers.length];
		for (int i = 0; i < spyHandlers.length; i++) {
			spyHandlers[i] = spy(handlers[i]);
			;
		}
		return spyHandlers;
	}

}
