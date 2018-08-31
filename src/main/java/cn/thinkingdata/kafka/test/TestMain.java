package cn.thinkingdata.kafka.test;


import java.io.IOException;
import java.net.URL;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.thinkingdata.kafka.close.ScanTermMethod;
import cn.thinkingdata.kafka.consumer.IDataLineProcessor;
import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumer;

public class TestMain {

	private static final Logger logger = LoggerFactory
			.getLogger(TestMain.class);

	static String jdbcUrl = "jdbc:mysql://mysql-test:3306/ta?autoReconnect=true&amp;useUnicode=true";
	static String dataProcessNum = "3";
	static KafkaSubscribeConsumer consumers;


	public static void main(String[] args) throws IOException, InterruptedException {

		String brokerList = args[0];
		String kafkaClusterName = args[1];
		String topic = args[2];
		String consumerGroup = args[3];
		String processThreadNum = args[4];
		String flushOffsetSize = args[5];
		String flushInterval = args[6];
		final String dataProcessNum = args[7];
		String maxPartitionFetchBytes = args[8];
		
		URL url = TestMain.class.getResource("/log4j.properties");
		PropertyConfigurator.configure(url);
		
		

		IDataLineProcessor dataProcessor = new IDataLineProcessor() {
			
			ThreadPoolExecutor executorService = new ThreadPoolExecutor(Integer.parseInt(dataProcessNum), Integer.parseInt(dataProcessNum),
	                0L, TimeUnit.MILLISECONDS,
	                new LinkedBlockingQueue<Runnable>(500));
			{
				executorService.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
			}

			@Override
			public void processData(String key, String value) {
				executorService.submit(new ProcessDataThread(key, value));
			}

			@Override
			public void finishProcess() {
				// 关闭线程池
				if (executorService != null)
					executorService.shutdown();
				try {
					if (!executorService.awaitTermination(5000,
							TimeUnit.MILLISECONDS)) {
						logger.warn("Timed out waiting for data process threads to shut down, exiting uncleanly");
					}
				} catch (InterruptedException e) {
					logger.warn("Interrupted during shutdown, exiting uncleanly");
				}
			}
		};


		if(maxPartitionFetchBytes == null){
			consumers = new KafkaSubscribeConsumer(jdbcUrl, "ta", "TaThinkingData",
					"kafka_consumer_offset", brokerList, kafkaClusterName, topic,
					consumerGroup, dataProcessor, Integer.parseInt(processThreadNum),
					Integer.parseInt(flushOffsetSize),
					Integer.parseInt(flushInterval),new ScanTermMethod());

		} else {
			consumers = new KafkaSubscribeConsumer(jdbcUrl, "ta", "TaThinkingData",
					"kafka_consumer_offset", brokerList, kafkaClusterName, topic,
					consumerGroup, dataProcessor, Integer.parseInt(processThreadNum),
					Integer.parseInt(flushOffsetSize),
					Integer.parseInt(flushInterval),Long.parseLong(maxPartitionFetchBytes),new ScanTermMethod());

		}
		
		consumers.run();
	}







}
