package cn.thinkingdata.kafka.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class ProcessDataThread implements Runnable {
	
	private static final Logger logger = LoggerFactory
			.getLogger(ProcessDataThread.class);
	
	public static AtomicLong messageCount = new AtomicLong(0L);
	
	private String key;
	private String value;

	public ProcessDataThread(String key, String value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public void run() {
		logger.info(key + "-----" + value);
		long count = messageCount.incrementAndGet();
		logger.info(Thread.currentThread().getName() + "-------" + count);
	}

}
