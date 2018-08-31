package cn.thinkingdata.kafka.consumer;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.offset.MysqlOffsetManager;
import cn.thinkingdata.kafka.consumer.offset.OffsetManager;
import cn.thinkingdata.kafka.consumer.persist.MysqlOffsetPersist;

public class KafkaSubscribeConsumeThread implements Runnable {

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaSubscribeConsumeThread.class);
	private IDataLineProcessor dataProcessor;
	public KafkaConsumer<String, String> consumer;
	private OffsetManager offsetManager = MysqlOffsetManager.getInstance();
	public volatile Boolean kafkaPollFlag = false;
	public volatile Boolean consumerDataRunFlag = false;
	public volatile Boolean kafkaConsumerFlag = false;
	public Set<KafkaConsumerOffset> kafkaConsumerOffsetSet = new HashSet<KafkaConsumerOffset>();

	public KafkaSubscribeConsumeThread(KafkaConsumer<String, String> consumer,
			IDataLineProcessor dataProcessor) {
		this.consumer = consumer;
		this.dataProcessor = dataProcessor;
	}

	@Override
	public void run() {
		kafkaConsumerFlag = true;
		Set<ConsumerRecord<String, String>> lastConsumerRecordSet = new HashSet<ConsumerRecord<String, String>>();
		Long count = 0L;
		try {
			while (!KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed
					.get()) {
				if (KafkaMysqlOffsetParameter.mysqlConnState.get()) {
					kafkaPollFlag = true;
					count = 0L;
					consumerDataRunFlag = false;
					// 如果有新的consumer，则调用rebalance，并阻塞线程
					ConsumerRecords<String, String> records = consumer
							.poll(1000);
					consumerDataRunFlag = true;
					if (records.count() > 0) {
						logger.debug("poll records size: " + records.count()
								+ ", partition is " + records.partitions()
								+ ", thread is "
								+ Thread.currentThread().getName());
					}
					for (ConsumerRecord<String, String> consumerRecord : records) {
						count++;
						dataProcessor.processData(consumerRecord.key(),
								consumerRecord.value());
						addLastConsumerRecord(lastConsumerRecordSet,
								consumerRecord);
					}
					// 更新offset
					for (ConsumerRecord<String, String> lastConsumerRecord : lastConsumerRecordSet) {
						Date now = new Date();
						KafkaConsumerOffset kafkaConsumerOffset = KafkaCache
								.searchKafkaConsumerOffset(
										lastConsumerRecord.topic(),
										lastConsumerRecord.partition());
						if (kafkaConsumerOffset == null) {
							logger.error("kafkaConsumerOffset is null in cache, the lastConsumerRecord is "
									+ lastConsumerRecord
									+ ", the kafkaConsumerOffsets is "
									+ KafkaCache.kafkaConsumerOffsets);
						}
						kafkaConsumerOffset
								.setTopic(lastConsumerRecord.topic());
						kafkaConsumerOffset.setPartition(lastConsumerRecord
								.partition());
						kafkaConsumerOffset
								.setConsumer_group(KafkaMysqlOffsetParameter.consumerGroup);
						kafkaConsumerOffset.setOffset(lastConsumerRecord
								.offset() + 1L);
						kafkaConsumerOffset
								.setKafka_cluster_name(KafkaMysqlOffsetParameter.kafkaClusterName);
						kafkaConsumerOffset.setCount(count);
						kafkaConsumerOffset.setUpdate_time(now);
						kafkaConsumerOffsetSet.add(kafkaConsumerOffset);
						offsetManager.saveOffsetInCache(kafkaConsumerOffset);
					}
					lastConsumerRecordSet.clear();
				} else {
					logger.info("mysql connect error, the mysqlConnState is "
							+ KafkaMysqlOffsetParameter.mysqlConnState.get());
					consumerDataRunFlag = false;
					kafkaPollFlag = false;
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						logger.error("------- thread can not sleep ---------------------"
								+ e.toString());
					}
				}
			}
			logger.info("kafka consumer close, the kafkaSubscribeConsumerClosed is "
					+ KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed
							.get());
			consumerDataRunFlag = false;
			kafkaPollFlag = false;
		} catch (Exception e) {
			// 外部thread中断kafka的poll操作
			logger.info("stop consumer with wakeup, the kafkaSubscribeConsumerClosed is "
					+ KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed
							.get()
					+ ", the thread is "
					+ Thread.currentThread().getName()
					+ " , the kafka cluster name is "
					+ KafkaMysqlOffsetParameter.kafkaClusterName
					+ ", consumerDataRunFlag is "
					+ consumerDataRunFlag
					+ ", the Exception is " + e.toString());
			// 更新offset
			for (ConsumerRecord<String, String> lastConsumerRecord : lastConsumerRecordSet) {
				Date now = new Date();
				KafkaConsumerOffset kafkaConsumerOffset = KafkaCache
						.searchKafkaConsumerOffset(lastConsumerRecord.topic(),
								lastConsumerRecord.partition());
				if (kafkaConsumerOffset == null) {
					logger.error("kafkaConsumerOffset is null in cache");
				}
				kafkaConsumerOffset.setTopic(lastConsumerRecord.topic());
				kafkaConsumerOffset
						.setPartition(lastConsumerRecord.partition());
				kafkaConsumerOffset
						.setConsumer_group(KafkaMysqlOffsetParameter.consumerGroup);
				kafkaConsumerOffset.setOffset(lastConsumerRecord.offset() + 1L);
				kafkaConsumerOffset
						.setKafka_cluster_name(KafkaMysqlOffsetParameter.kafkaClusterName);
				kafkaConsumerOffset.setCount(count);
				logger.info("save the value with consumer WakeupException"
						+ Thread.currentThread().getName()
						+ ", the kafkaConsumerOffset is "
						+ kafkaConsumerOffset.toString());
				kafkaConsumerOffsetSet.add(kafkaConsumerOffset);
				offsetManager.saveOffsetInCache(kafkaConsumerOffset);
			}
			consumerDataRunFlag = false;
			kafkaPollFlag = false;
		} finally {
			logger.info("wait for the mysql persist finish");
			// 等待MysqlOffsetPersist的persist动作完成
			for (;;) {
				if (!MysqlOffsetPersist.runFlag)
					break;
			}
			logger.info("flush before kafka consumer close");
			logger.debug("kafkaConsumerOffsets is "
					+ KafkaCache.kafkaConsumerOffsets);
			for (KafkaConsumerOffset kafkaConsumerOffset : kafkaConsumerOffsetSet) {
				KafkaConsumerOffset kafkaConsumerOffsetInCache = KafkaCache
						.searchKafkaConsumerOffset(
								kafkaConsumerOffset.getTopic(),
								kafkaConsumerOffset.getPartition());
				MysqlOffsetPersist.getInstance().flush(
						kafkaConsumerOffsetInCache);
			}
			logger.info("kafka consumer finally close");
			consumer.close();
			kafkaConsumerFlag = false;
		}
	}

	private KafkaConsumerOffset searchKafkaConsumerOffsetSet(
			Set<KafkaConsumerOffset> kafkaConsumerOffsetSet,
			KafkaConsumerOffset kafkaConsumerOffset) {
		for (KafkaConsumerOffset KafkaConsumerOffsetInSet : kafkaConsumerOffsetSet) {
			if (KafkaConsumerOffsetInSet.getTopic().equals(
					kafkaConsumerOffset.getTopic())
					&& KafkaConsumerOffsetInSet.getPartition() == kafkaConsumerOffset
							.getPartition()) {
				return KafkaConsumerOffsetInSet;
			}
		}
		return null;
	}

	private void addLastConsumerRecord(
			Set<ConsumerRecord<String, String>> lastConsumerRecordSet,
			ConsumerRecord<String, String> consumerRecord) {
		ConsumerRecord<String, String> consumerRecordInSet = searchConsumerRecord(
				lastConsumerRecordSet, consumerRecord);
		if (consumerRecordInSet == null) {
			lastConsumerRecordSet.add(consumerRecord);
		} else {
			lastConsumerRecordSet.remove(consumerRecordInSet);
			lastConsumerRecordSet.add(consumerRecord);
		}
	}

	private ConsumerRecord<String, String> searchConsumerRecord(
			Set<ConsumerRecord<String, String>> lastConsumerRecordSet,
			ConsumerRecord<String, String> consumerRecord) {
		for (ConsumerRecord<String, String> consumerRecordInSet : lastConsumerRecordSet) {
			if (consumerRecordInSet.topic().equals(consumerRecord.topic())
					&& consumerRecordInSet.partition() == consumerRecord
							.partition()) {
				return consumerRecordInSet;
			}
		}
		return null;
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed.set(true);
		consumer.wakeup();
	}

}
