package cn.thinkingdata.kafka.close;

import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DaemonCloseThread extends Thread {

	private static final Logger logger = LoggerFactory
			.getLogger(DaemonCloseThread.class);
	
	KafkaSubscribeConsumer consumers;
	
	TermMethod closeMethod;
	
	Boolean flag = true;
	
	public DaemonCloseThread(KafkaSubscribeConsumer consumers, TermMethod closeMethod){
		this.consumers=consumers;
		this.closeMethod=closeMethod;
	}
	
	public static void destroy(KafkaSubscribeConsumer consumers) {
		consumers.destroy();
	}

	@Override
	public void run() {
		close();
	}
	
	public void shutdown() {
		flag = false;
	}
	
	public void afterDestroyConsumer() {
		closeMethod.afterDestroyConsumer();
	}


	private void close() {
		logger.info("start DaemonCloseThread!");
		while(flag){
			Boolean receiveTermSignal = closeMethod.receiveTermSignal();
			if(receiveTermSignal){
				logger.info("start to destroy consumers");
				destroy(consumers);
				break;
			} else {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					logger.error("------- thread can not sleep ---------------------"
							+ e.toString());
				}
			}
		}
		logger.info("DaemonCloseThread stop!");
	}
}
