package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.jd.blockchain.consensus.service.NodeState;

import test.com.jd.blockchain.consensus.bftsmart.BftsmartNodeStateVerifier.LCStatusOption;

/**
 * 节点状态测试；
 * <p>
 * 
 * 测试一个共识环境的所有运行中的节点的领导者状态是否一致；
 * 
 * @author huanghaiquan
 *
 */
public class NodeStateTestcase implements ConsensusTestcase {

	private List<Comparator<NodeState>> stateComparators = new LinkedList<Comparator<NodeState>>();

	private boolean requireVerifier = false;

	private StateVerifier stateVerifier;
	
	
	public NodeStateTestcase() {
	}
	
	@SuppressWarnings("unchecked")
	public static NodeStateTestcase createNormalConsistantTest() {
		NodeStateTestcase stateTest = new NodeStateTestcase();
		
		BftsmartNodeStateVerifier verifier = new BftsmartNodeStateVerifier();
		verifier.setCheckingConsensusQuorum(false);// 暂时不校验“法定数量”属性；
		verifier.setLCStatusOption(LCStatusOption.NORMAL);// 暂时不校验“下一个执政ID”属性；
		stateTest.setStateVerifier(verifier);
		stateTest.setRequireVerifier(true);

		stateTest.setConsistantComparators(new BftsmartNodeStateComparator());
		return stateTest;
	}
	

	/**
	 * 运行节点状态测试；
	 */
	@Override
	public void run(ConsensusEnvironment environment) {
		// 收集运行节点的内部状态；
		ReplicaNodeServer[] replicaNodes = environment.getRunningNodes();
		if (replicaNodes.length == 0) {
			throw new IllegalStateException("No running node!");
		}

		NodeState[] nodeStates = new NodeState[replicaNodes.length];
		for (int i = 0; i < nodeStates.length; i++) {
			nodeStates[i] = replicaNodes[i].getNodeServer().getState();

			// 检查运行状态是否一致；
			assertTrue("The running state is not consistant with the NodeServer[%s]!", nodeStates[i].isRunning());
		}

		// 验证状态有效性；
		if (requireVerifier && stateVerifier == null) {
			throw new IllegalStateException("The state verifier is required but not set!");
		}
		if (stateVerifier != null) {
			for (int i = 0; i < nodeStates.length; i++) {
				stateVerifier.verify(replicaNodes[i], nodeStates[i]);
			}
		}

		// 检查状态的一致性；
		NodeState firstNodeState = nodeStates[0];
		for (int i = 1; i < nodeStates.length; i++) {
			for (Comparator<NodeState> comparator : stateComparators) {
				int r = comparator.compare(firstNodeState, nodeStates[i]);
				assertEquals(String.format("State of Node[%s] is not equals to the expected state!",
						replicaNodes[i].getReplica().getId()), 0, r);
			}
		}
	}

	/**
	 * 设置用于状态一致性比较器；
	 * 
	 * @param comparators
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public void setConsistantComparators(Comparator<NodeState>... comparators) {
		stateComparators.clear();
		if (comparators != null) {
			for (int i = 0; i < comparators.length; i++) {
				stateComparators.add(comparators[i]);
			}
		}
	}

	/**
	 * 设置状态验证器；
	 * 
	 * @param stateVerifier
	 */
	public void setStateVerifier(StateVerifier stateVerifier) {
		this.stateVerifier = stateVerifier;
	}

	/**
	 * 是否必须要求设置验证器；
	 * <p>
	 * 默认为 false；
	 * 
	 * @return
	 */
	public boolean isRequireVerifier() {
		return requireVerifier;
	}

	/**
	 * 设置是否必须要求设置验证器；
	 * <p>
	 * 默认为 false；
	 * 
	 * @param requireVerifier
	 */
	public void setRequireVerifier(boolean requireVerifier) {
		this.requireVerifier = requireVerifier;
	}

	/**
	 * 状态验证器；
	 * 
	 * @author huanghaiquan
	 *
	 */
	public static interface StateVerifier {

		/**
		 * 验证指定节点服务器的状态有效性；
		 * 
		 * @param node
		 * @param state
		 */
		void verify(ReplicaNodeServer node, NodeState state);

	}
}
