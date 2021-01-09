package test.perf.com.jd.blockchain.consensus.client;

import com.jd.httpservice.HttpAction;
import com.jd.httpservice.HttpMethod;
import com.jd.httpservice.HttpService;

/**
 * Created by zhangshuang3 on 2018/9/11.
 */
@HttpService
public interface ConsensusSettingService {

    @HttpAction(path = "/node/settings", method = HttpMethod.GET)
    public String getConsensusSettingsHex();

    @HttpAction(path = "/node/topology", method = HttpMethod.GET)
    public String getConsensusTopologyHex();

}

