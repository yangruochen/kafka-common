package cn.thinkingdata.kafka.test;


import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestMain2 {

	private static final Logger logger = LoggerFactory
			.getLogger(TestMain2.class);

	static String jdbcUrl = "jdbc:mysql://mysql-test:3306/ta?autoReconnect=true&amp;useUnicode=true";
	static String dataProcessNum = "3";


	public static void main(String[] args) throws IOException, InterruptedException {
//		String path = System.getProperty("user.dir")
//				+ "/src/main/resources/log4j.properties";
//		String path = TestMain.class.getClassLoader().getResource("log4j.properties").getPath();
//
//	    Properties props = new Properties();
//	    props.load(new FileInputStream(path));

		String zookeeperServers = args[0];
		String topic = args[1];
		String consumerGroup = args[2];
		String processThreadNum = args[3];
		final String dataProcessNum = args[4];
		
		URL url = TestMain2.class.getResource("/log4j.properties");
		PropertyConfigurator.configure(url);
		
		

//		IDataLineProcessor dataProcessor = new IDataLineProcessor() {
////			ExecutorService executorService = Executors
////					.newFixedThreadPool(Integer.parseInt(dataProcessNum));
//
//			ThreadPoolExecutor executorService = new ThreadPoolExecutor(Integer.parseInt(dataProcessNum), Integer.parseInt(dataProcessNum),
//	                0L, TimeUnit.MILLISECONDS,
//	                new LinkedBlockingQueue<Runnable>(500));
//			{
//				executorService.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
//			}
//
//			@Override
//			public void processData(String key, String value) {
//				executorService.submit(new ProcessDataThread(key, value));
//			}
//
//			@Override
//			public void finishProcess() {
//				// 关闭线程池
//				if (executorService != null)
//					executorService.shutdown();
//				try {
//					if (!executorService.awaitTermination(5000,
//							TimeUnit.MILLISECONDS)) {
//						logger.warn("Timed out waiting for data process threads to shut down, exiting uncleanly");
//					}
//					// executorService.shutdownNow();
//				} catch (InterruptedException e) {
//					logger.warn("Interrupted during shutdown, exiting uncleanly");
//				}
//			}
//		};



//		 consumers = new KafkaSubscribeConsumer(jdbcUrl, "ta",
//		 "TaThinkingData",
//		 "kafka_consumer_offset", "test:9092", "test", "test12",
//		 "testyrc", dataProcessor, 3, 3, 1);

		//		consumers = new KafkaSubscribeConsumer(jdbcUrl, "ta", "TaThinkingData",
//				"kafka_consumer_offset", "app:9092,app2:9092", "app", "test1",
//				"testyrc", dataProcessor, 3,
//				3,
//				1);
		

//		DaemonCloseThread closeSignal = new DaemonCloseThread(consumers, new ScanTermMethod());
//		DaemonCloseThread closeSignal = new DaemonCloseThread(consumers, new SignalTermMethod());
		
		
		// Signal interruptSignal = new Signal("INT");
		
//		Thread.sleep(600000);
//		consumers.shutdown();
		
	}







}
