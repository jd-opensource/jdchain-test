package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import com.jd.blockchain.consensus.NodeNetworkAddress;
import com.jd.blockchain.consensus.Replica;
import com.jd.blockchain.consensus.bftsmart.service.BftsmartNodeState;
import com.jd.blockchain.consensus.bftsmart.service.BftsmartNodeState.ViewState;
import com.jd.blockchain.consensus.service.NodeState;

import test.com.jd.blockchain.consensus.bftsmart.NodeStateTestcase.StateVerifier;

public class BftsmartNodeStateVerifier implements StateVerifier {

	/**
	 * 领导者选举状态的检测选项；
	 * 
	 * @author huanghaiquan
	 *
	 */
	public static enum LCStatusOption {

		/**
		 * 选举中；
		 */
		IN_PROGRESS,

		/**
		 * 未选举；
		 */
		NORMAL,

		/**
		 * 忽略；
		 */
		IGNORE
	}

	private boolean checkingConsensusQuorum = true;

	private LCStatusOption lcStatusOption = LCStatusOption.IGNORE;

	public BftsmartNodeStateVerifier() {
	}

	@Override
	public void verify(ReplicaNodeServer node, NodeState state) {
		Replica replica = node.getReplica();
		BftsmartNodeState bftState = (BftsmartNodeState) state;

		// 检查节点ID分配是否正确；
		// 通过对比外部设置的 ID 和内部状态实际的 ID；
		assertEquals(replica.getId(), bftState.getNodeID());
		assertEquals(replica.getId(), bftState.getViewState().getStaticConfProccessID());

		// 检查节点ID属于视图的节点ID清单范围
		int[] viewProcessIDs = bftState.getViewState().getProcessIDs();
		boolean nodeIdIllegal = Arrays.stream(viewProcessIDs).anyMatch(id -> id == replica.getId());
		assertTrue("The id of current replica node is not in the process id list of view state!", nodeIdIllegal);

		// 验证网络地址-配置一致；
//		NodeNetworkAddress[] networkAddresses = bftState.getViewState().getProcessNetAddresses();
//		NodeNetworkAddress nodeReallyNetworkAddress = null;
//		for (int i = 0; i < viewProcessIDs.length; i++) {
//			if (viewProcessIDs[i] == replica.getId()) {
//				nodeReallyNetworkAddress = networkAddresses[i];
//			}
//		}
//		assertEquals(replica.getNetworkAddress().getHost(), nodeReallyNetworkAddress.getHost());
//		assertEquals(replica.getNetworkAddress().getPort(), nodeReallyNetworkAddress.getConsensusPort());

		// 检查视图的有效性；
		verifyViewState(bftState.getViewState());

		// 检查领导者状态：检查领导者 ID 是否属于视图定义的节点列表；
		boolean leaderIdIllegal = Arrays.stream(viewProcessIDs)
				.anyMatch(id -> id == bftState.getLeaderState().getLeaderID());
		assertTrue("Leader id in LeaderState is not contain in the process id list of ViewState!", leaderIdIllegal);

		// 检查领导者的执政ID是否已经初始化；
		assertTrue(
				"The last regency id is negative! The LeaderState has not been initialized! --[last-regency-id="
						+ bftState.getLeaderState().getLastRegency() + "]",
				bftState.getLeaderState().getLastRegency() > -1);

		// 检查领导者状态的下一个执政ID是否有效；
		if (lcStatusOption != LCStatusOption.IGNORE) {
			assertFalse("The LC Status of node[" + bftState.getNodeID() + "] is illegal!",
					bftState.getLeaderState().getLastRegency() > bftState.getLeaderState().getNextRegency());

			switch (lcStatusOption) {
			case IN_PROGRESS:
				assertTrue("Expected the LC Status of node[" + bftState.getNodeID() + "] is in progress, but not!",
						bftState.getLeaderState().getLastRegency() < bftState.getLeaderState().getNextRegency());
				break;
			case NORMAL:
				assertTrue(
						"Expected the LC Status of node[" + bftState.getNodeID() + "] is not in progress, but it is!",
						bftState.getLeaderState().getLastRegency() == bftState.getLeaderState().getNextRegency());
				break;
			default:
				throw new IllegalStateException("Unsupported LCStatusOption [" + lcStatusOption + "]!");
			}
		}

		// 检查领导者状态和共识状态的领导者 ID 是否一致；
		assertEquals("The leader ids in LeaderState and ConsensusState are inconsistant!",
				bftState.getLeaderState().getLeaderID(), bftState.getConsensusState().getLeaderID());

		// 检查通讯状态的存活情况与节点的运行状态是否一致；
		// 对比通讯线程的状态；
		assertEquals(node.getNodeServer().isRunning(), bftState.getCommunicationState().isTomLayerThreadAlived());
		// 对比通讯层标记的运行状态；
		assertEquals(node.getNodeServer().isRunning(), bftState.getCommunicationState().isTomLayerRunning());
	}

	/**
	 * 检查视图的有效性；
	 * 
	 * @param viewState
	 */
	private void verifyViewState(ViewState viewState) {
		// 检查视图 ID，预期视图是已经正确进行了初始化；
		assertTrue(
				"View id is negative! The view state has not been initialized! [viewId=" + viewState.getViewID() + "]",
				viewState.getViewID() > -1);

		// 检查视图属性；
		int[] viewProcessIDs = viewState.getProcessIDs();
		NodeNetworkAddress[] networkAddresses = viewState.getProcessNetAddresses();
		assertEquals(viewState.getViewN(), viewProcessIDs.length);
		assertEquals(viewState.getViewN(), networkAddresses.length);

		// 检查视图的节点ID清单中是否存在重复项；
		Arrays.sort(viewProcessIDs);
		for (int i = 1; i < viewProcessIDs.length; i++) {
			assertNotEquals("There are some repeatly ids in the process id list of View!", viewProcessIDs[i - 1],
					viewProcessIDs[i]);
		}

		// 验证容错数；
		int f = computeFailureThreshold(viewState.getViewN());
		assertEquals("The failure number of View is wrong!", f, viewState.getViewF());

		// 验证法定数；
		if (checkingConsensusQuorum) {
			int quorum = viewState.getViewN() - f;
			assertEquals("Wrong quorum for total node count[" + viewState.getViewN() + "]!", quorum,
					viewState.getQuorum());
		}
	}

	/**
	 * 计算在指定节点数量下的拜占庭容错数；
	 * 
	 * @param nodeNumber
	 * @return
	 */
	public static int computeFailureThreshold(int nodeNumber) {
		if (nodeNumber < 1) {
			throw new IllegalArgumentException("Node number is less than 1!");
		}
		int f = 0;
		while (true) {
			if (nodeNumber >= (3 * f + 1) && nodeNumber < (3 * (f + 1) + 1)) {
				break;
			}
			f++;
		}
		return f;
	}

	/**
	 * 是否检查共识的法定数量；
	 * 
	 * @return
	 */
	public boolean isCheckingConsensusQuorum() {
		return checkingConsensusQuorum;
	}

	public void setCheckingConsensusQuorum(boolean checkingConsensusQuorum) {
		this.checkingConsensusQuorum = checkingConsensusQuorum;
	}
	
	public LCStatusOption getLCStatusOption() {
		return lcStatusOption;
	}
	
	public void setLCStatusOption(LCStatusOption lcStatusOption) {
		this.lcStatusOption = lcStatusOption;
	}

	/**
	 * 返回指定环境中的领导者节点；
	 * <p>
	 * 
	 * 如果检测到超过 1 个共识节点，则抛出异常；
	 * 
	 * <p>
	 * 如果没有检测到领导者节点，则返回 null；
	 * 
	 * @param environment
	 * @return
	 */
	public static ReplicaNodeServer getLeader(ConsensusEnvironment environment) {
		ReplicaNodeServer[] runningNodes = environment.getRunningNodes();
		ReplicaNodeServer leader = null;
		for (int i = 0; i < runningNodes.length; i++) {
			BftsmartNodeState bftstate = (BftsmartNodeState) runningNodes[i].getNodeServer().getState();
			if (bftstate.isLeader()) {
				if (leader == null) {
					leader = runningNodes[i];
				} else {
					throw new IllegalStateException("More than one leader in the specified consensus environment!");
				}
			}
		}
		return leader;
	}
}
