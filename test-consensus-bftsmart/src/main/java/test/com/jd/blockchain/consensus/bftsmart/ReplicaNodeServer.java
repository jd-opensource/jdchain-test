package test.com.jd.blockchain.consensus.bftsmart;

import com.jd.blockchain.consensus.Replica;
import com.jd.blockchain.consensus.service.NodeServer;

public interface ReplicaNodeServer {

	Replica getReplica();

	NodeServer getNodeServer();

}
