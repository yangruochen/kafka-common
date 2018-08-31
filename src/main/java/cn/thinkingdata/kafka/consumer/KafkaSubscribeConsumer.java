package cn.thinkingdata.kafka.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.close.DaemonCloseThread;
import cn.thinkingdata.kafka.close.SignalTermMethod;
import cn.thinkingdata.kafka.close.TermMethod;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.persist.MysqlOffsetPersist;

public class KafkaSubscribeConsumer {

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaSubscribeConsumer.class);

	protected IDataLineProcessor dataProcessor;
	protected ExecutorService executorService;
	private TermMethod closeMethod;

	// 同步offset的size，同步offset的时间
	public KafkaSubscribeConsumer(String jdbcUrl, String username,
			String password, String tableName, String brokerList,
			String kafkaClusterName, String topic, String consumerGroup,
			IDataLineProcessor dataProcessor, Integer processThreadNum,
			Integer flushOffsetSize, Integer flushInterval,
			TermMethod closeMethod) throws IOException {
		KafkaMysqlOffsetParameter.setValue(jdbcUrl, username, password,
				tableName, brokerList, kafkaClusterName, topic, consumerGroup,
				processThreadNum, flushOffsetSize, flushInterval);
		this.dataProcessor = dataProcessor;
		this.closeMethod = closeMethod;
	}

	public KafkaSubscribeConsumer(String jdbcUrl, String username,
			String password, String tableName, String brokerList,
			String kafkaClusterName, String topic, String consumerGroup,
			IDataLineProcessor dataProcessor, Integer processThreadNum,
			Integer flushOffsetSize, Integer flushInterval, Long maxPartitionFetchBytes,
			TermMethod closeMethod) throws IOException {
		KafkaMysqlOffsetParameter.setValue(jdbcUrl, username, password,
				tableName, brokerList, kafkaClusterName, topic, consumerGroup,
				processThreadNum, flushOffsetSize, flushInterval);
		KafkaMysqlOffsetParameter.setMaxPartitionFetchBytes(maxPartitionFetchBytes);
		this.dataProcessor = dataProcessor;
		this.closeMethod = closeMethod;
	}

	public void run() {
		KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed.set(false);
		Properties kafkaConf = new Properties();
		kafkaConf
				.put("bootstrap.servers", KafkaMysqlOffsetParameter.brokerList);
		kafkaConf.put("group.id", KafkaMysqlOffsetParameter.consumerGroup);
		// Below is a key setting to turn off the auto commit.
		kafkaConf.put("enable.auto.commit", "false");
		kafkaConf.put("heartbeat.interval.ms", "5000");
		kafkaConf.put("session.timeout.ms", "15001");
		// Control maximum data on each poll, make sure this value is bigger
		// than the maximum single record size
		kafkaConf.put("max.partition.fetch.bytes",
				KafkaMysqlOffsetParameter.maxPartitionFetchBytes);
		kafkaConf.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConf.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		List<String> topicList = new ArrayList<String>();
		topicList.add(KafkaMysqlOffsetParameter.topic);
		executorService = Executors
				.newFixedThreadPool(KafkaMysqlOffsetParameter.processThreadNum);
		for (int i = 0; i < KafkaMysqlOffsetParameter.processThreadNum; i++) {
			KafkaSubscribeConsumerManager kafkaSubscribeConsumer = KafkaSubscribeConsumerManager
					.getInstance();
			KafkaConsumer<String, String> consumer = kafkaSubscribeConsumer
					.createKafkaConsumer(topicList, kafkaConf);
			KafkaSubscribeConsumeThread consumeThread = new KafkaSubscribeConsumeThread(
					consumer, dataProcessor);
			KafkaCache.consumeThreadList.add(consumeThread);
			executorService.submit(consumeThread);
		}
		// 启动定时刷数据入mysql
		MysqlOffsetPersist.getInstance().start();
		DaemonCloseThread closeSignal = new DaemonCloseThread(this,closeMethod);
		closeSignal.setDaemon(true);
		closeSignal.start();
	}

	public void shutdown() {
		logger.info("consumers start shutdown");
		for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
			consumeThread.shutdown();
		}
		// 等待所有拉取线程自动停止
		for (;;) {
			Boolean kafkaPollFlag = false;
			for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
				if (consumeThread != null && consumeThread.kafkaPollFlag) {
					kafkaPollFlag = true;
				}
			}
			if (!kafkaPollFlag) {
				break;
			}
		}
		logger.info("kafka polling closed");
		// 等待所有consumer关闭
		for (;;) {
			Boolean kafkaConsumerFlag = false;
			for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
				if (consumeThread != null && consumeThread.kafkaConsumerFlag) {
					kafkaConsumerFlag = true;
				}
			}
			if (!kafkaConsumerFlag) {
				break;
			}
		}
		logger.info("kafka consumer closed");
		// 关闭线程池
		if (executorService != null)
			executorService.shutdown();
		try {
			if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				logger.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			logger.warn("Interrupted during shutdown, exiting uncleanly");
		}
		logger.info("dataProcessor start to shutdown");
		dataProcessor.finishProcess();
		logger.info("mysql start to shutdown");
		// 关闭mysql定时任务
		MysqlOffsetPersist.getInstance().shutdown();

	}

}
