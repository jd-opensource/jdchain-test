package test.com.jd.blockchain.consensus.bftsmart;

import com.jd.blockchain.consensus.client.ConsensusClient;

import utils.concurrent.AsyncHandle;

/**
 * 消息请求；
 * <p>
 * 
 * 表示 {@link MessageConsensusTestcase} 一次测试中一个客户端发起的消息请求；
 * 
 * @author huanghaiquan
 *
 */
public interface MessageRequest {

	int getMessageId();

	/**
	 * 发送的消息；
	 * 
	 * @return
	 */
	byte[] getMessage();

	/**
	 * 是否已经收到回复；
	 * 
	 * @return
	 */
	boolean isCompleted();

	/**
	 * 是否发生了异常；
	 * 
	 * @return
	 */
	boolean isExceptionally();

	/**
	 * 收到的回复；
	 * <p>
	 * 
	 * 如果正在等待回复，则调用此方法会堵塞，直至收到回复( {@link #isCompleted()} 为 true )；
	 * 
	 * @param timeout 最大的等待时长；
	 * @return
	 */
	byte[] getResponse(long timeout);

	/**
	 * 是否已经完成发送；
	 * 
	 * @return
	 */
	boolean isSended();

	/**
	 * 与此次请求关联的客户端；
	 * 
	 * @return
	 */
	ConsensusClient getClient();

	/**
	 * 当操作完成的时候调用参数 handle 指定的处理；
	 * 
	 * <p>
	 * 
	 * 如果调用此方法时操作已完成，则在此方法返回之前完成调用参数 handle 指定的处理；
	 * 
	 * @param handle
	 */
	void onCompleted(AsyncHandle<byte[]> handle);
}
