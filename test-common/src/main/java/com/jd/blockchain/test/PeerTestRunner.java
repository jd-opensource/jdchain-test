package com.jd.blockchain.test;

import com.jd.blockchain.ledger.core.LedgerManager;
import com.jd.blockchain.peer.PeerServerBooter;
import com.jd.blockchain.storage.service.DbConnectionFactory;
import com.jd.blockchain.tools.initializer.LedgerBindingConfig;
import com.jd.blockchain.utils.concurrent.ThreadInvoker;
import com.jd.blockchain.utils.concurrent.ThreadInvoker.AsyncCallback;
import com.jd.blockchain.utils.net.NetworkAddress;

/**
 * PeerServer 定义了一个 Peer 节点服务器实例；
 * 
 * @author huanghaiquan
 *
 */
public class PeerTestRunner {

	private NetworkAddress serviceAddress;

	private volatile PeerServerBooter booter;

	private LedgerBindingConfig ledgerBindingConfig;

	public DbConnectionFactory getDBConnectionFactory() {
		return booter.getDBConnectionFactory();
	}

	public NetworkAddress getServiceAddress() {
		return serviceAddress;
	}

	public LedgerBindingConfig getLedgerBindingConfig() {
		return ledgerBindingConfig;
	}

	public PeerTestRunner(NetworkAddress serviceAddress, LedgerBindingConfig ledgerBindingConfig) {
		this(serviceAddress, ledgerBindingConfig, null, null);
	}
	
	public PeerTestRunner(NetworkAddress serviceAddress, LedgerBindingConfig ledgerBindingConfig, DbConnectionFactory dbConnectionFactory) {
		this(serviceAddress, ledgerBindingConfig, dbConnectionFactory, null);
	}

	public PeerTestRunner(NetworkAddress serviceAddress, LedgerBindingConfig ledgerBindingConfig,
			DbConnectionFactory dbConnectionFactory, LedgerManager ledgerManager) {
		this.serviceAddress = serviceAddress;
		this.ledgerBindingConfig = ledgerBindingConfig;
		if (dbConnectionFactory == null) {
			this.booter = new PeerServerBooter(ledgerBindingConfig, serviceAddress.getHost(),
					serviceAddress.getPort());
		} else {
			this.booter = new PeerServerBooter(ledgerBindingConfig, serviceAddress.getHost(),
					serviceAddress.getPort(), dbConnectionFactory, ledgerManager);
		}
	}

	public AsyncCallback<Object> start() {
		ThreadInvoker<Object> invoker = new ThreadInvoker<Object>() {
			@Override
			protected Object invoke() throws Exception {
				booter.start();

				return null;
			}
		};

		return invoker.start();
	}

	public void stop() {
		booter.close();
	}
}
