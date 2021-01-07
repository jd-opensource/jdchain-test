package test.com.jd.blockchain.consensus.bftsmart;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;
import java.util.Objects;

import org.bouncycastle.util.Arrays;

import com.jd.blockchain.consensus.NodeNetworkAddress;
import com.jd.blockchain.consensus.bftsmart.service.BftsmartNodeState;
import com.jd.blockchain.consensus.bftsmart.service.BftsmartNodeState.CommunicationState;
import com.jd.blockchain.consensus.bftsmart.service.BftsmartNodeState.ConsensusState;
import com.jd.blockchain.consensus.bftsmart.service.BftsmartNodeState.LeaderState;
import com.jd.blockchain.consensus.bftsmart.service.BftsmartNodeState.ViewState;
import com.jd.blockchain.consensus.service.NodeState;

public class BftsmartNodeStateComparator implements Comparator<NodeState> {

	@Override
	public int compare(NodeState state1, NodeState state2) {
		BftsmartNodeState bftState1 = (BftsmartNodeState) state1;
		BftsmartNodeState bftState2 = (BftsmartNodeState) state2;

		return compareBFTState(bftState1, bftState2);
	}

	/**
	 * 比较 BFT 状态；
	 * <p>
	 * 
	 * 只检查那些具有公共一致性的属性，而不检查本地的个性状态，例如本节点 ID 这类；
	 * <p>
	 * 
	 * @param bftState1
	 * @param bftState2
	 * @return
	 */
	private int compareBFTState(BftsmartNodeState bftState1, BftsmartNodeState bftState2) {
		int r = compareViewState(bftState1.getViewState(), bftState2.getViewState());
		assertTrue(String.format("View states of node[%s] and node[%s] are inconsistant!", bftState1.getNodeID(),
				bftState2.getNodeID()), r == 0);

		r = compareLeaderState(bftState1.getLeaderState(), bftState2.getLeaderState());
		assertTrue(String.format("Leader states of node[%s] and node[%s] are inconsistant!", bftState1.getNodeID(),
				bftState2.getNodeID()), r == 0);

		r = compareConsensusState(bftState1.getConsensusState(), bftState2.getConsensusState());
		assertTrue(String.format("Consensus states of node[%s] and node[%s] are inconsistant!", bftState1.getNodeID(),
				bftState2.getNodeID()), r == 0);

		r = compareCommunicationState(bftState1.getCommunicationState(), bftState2.getCommunicationState());
		assertTrue(String.format("Communication states of node[%s] and node[%s] are inconsistant!",
				bftState1.getNodeID(), bftState2.getNodeID()), r == 0);

		return 0;
	}

	private int compareCommunicationState(CommunicationState communicationState1,
			CommunicationState communicationState2) {
		// 通讯状态不需要比较一致性；
		return 0;
	}

	private int compareConsensusState(ConsensusState consensusState1, ConsensusState consensusState2) {
		if (consensusState1.getLastConensusID() == consensusState2.getLastConensusID()) {
			return 0;
		}
		return -1;
	}

	private int compareLeaderState(LeaderState leaderState1, LeaderState leaderState2) {
		if (leaderState1.getLeaderID() == leaderState2.getLeaderID()
				&& leaderState1.getLastRegency() == leaderState2.getLastRegency()
				&& leaderState1.getNextRegency() == leaderState2.getNextRegency()) {
			return 0;
		}
		return -1;
	}

	private int compareViewState(ViewState viewState1, ViewState viewState2) {
		if (viewState1.getViewID() != viewState2.getViewID()) {
			return -1;
		}
		if (viewState1.getViewN() != viewState2.getViewN()) {
			return -1;
		}
		if (viewState1.getViewF() != viewState2.getViewF()) {
			return -1;
		}
		if (viewState1.getQuorum() != viewState2.getQuorum()) {
			return -1;
		}
		if (!Arrays.areEqual(viewState1.getProcessIDs(), viewState2.getProcessIDs())) {
			return -1;
		}
		
		NodeNetworkAddress[] address1 = viewState1.getProcessNetAddresses();
		NodeNetworkAddress[] address2 = viewState2.getProcessNetAddresses();
		if (address1.length != address2.length) {
			return -1;
		}
		for (int i = 0; i < address1.length; i++) {
			if (!Objects.equals(address1[i].getHost(), address2[i].getHost())) {
				return -1;
			}
			if (address1[i].getConsensusPort() != address2[i].getConsensusPort()) {
				return -1;
			}
			if (address1[i].getMonitorPort() != address2[i].getMonitorPort()) {
				return -1;
			}
		}
		
		return 0;
	}

}
