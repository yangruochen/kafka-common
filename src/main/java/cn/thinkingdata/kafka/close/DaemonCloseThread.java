package cn.thinkingdata.kafka.close;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumer;

public class DaemonCloseThread extends Thread {

	private static final Logger logger = LoggerFactory
			.getLogger(DaemonCloseThread.class);
	
	static KafkaSubscribeConsumer consumers;
	
	static TermMethod closeMethod;
	
	public DaemonCloseThread(KafkaSubscribeConsumer consumers, TermMethod closeMethod){
		this.consumers=consumers;
		this.closeMethod=closeMethod;
	}
	
	public static void destroy(KafkaSubscribeConsumer consumers) {
		consumers.shutdown();
	}

	@Override
	public void run() {
		close(closeMethod);
	}

	private void close(TermMethod closeMethod) {
		while(true){
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
		closeMethod.afterDestroyConsumer();
	}
}
