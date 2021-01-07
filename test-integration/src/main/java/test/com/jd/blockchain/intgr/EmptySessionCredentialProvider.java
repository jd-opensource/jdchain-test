package test.com.jd.blockchain.intgr;

import com.jd.blockchain.consensus.SessionCredential;
import com.jd.blockchain.sdk.service.SessionCredentialProvider;

public class EmptySessionCredentialProvider implements SessionCredentialProvider {

	public static final SessionCredentialProvider INSTANCE = new EmptySessionCredentialProvider();
	
	private EmptySessionCredentialProvider() {
	}

	@Override
	public SessionCredential getCredential(String key) {
		return null;
	}

	@Override
	public void setCredential(String key, SessionCredential credential) {
	}

}
