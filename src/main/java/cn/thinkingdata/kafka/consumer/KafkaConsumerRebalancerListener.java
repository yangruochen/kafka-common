package cn.thinkingdata.kafka.consumer;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.offset.MysqlOffsetManager;
import cn.thinkingdata.kafka.consumer.offset.OffsetManager;
import cn.thinkingdata.kafka.consumer.persist.MysqlOffsetPersist;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

/**
 * Re-balancer for any subscription changes.
 */
public class KafkaConsumerRebalancerListener implements
		org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

	private static KafkaConsumerRebalancerListener instance;

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaConsumerRebalancerListener.class);

	private OffsetManager offsetManager = MysqlOffsetManager.getInstance();

	private KafkaConsumer<String, String> consumer;

	public KafkaConsumerRebalancerListener(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}
	

	// onPartitionsRevoked 这个方法会在 Consumer 停止拉取数据之后、group 进行 rebalance
	// 操作之前调用，作用是对已经 ack 的 msg 进行 commit；
	@Override
	public void onPartitionsRevoked(
			Collection<TopicPartition> partitions) {
		logger.info("start onPartitionsRevoked!");
		for (TopicPartition partition : partitions) {
			KafkaConsumerOffset kafkaConsumerOffset = KafkaCache
					.searchKafkaConsumerOffset(partition.topic(),
							partition.partition());
			if (kafkaConsumerOffset != null) {
				kafkaConsumerOffset.setOffset(consumer
						.position(partition));
				KafkaCache.kafkaConsumerOffsets.add(kafkaConsumerOffset);
				MysqlOffsetPersist.getInstance().flush(kafkaConsumerOffset);
				//删除kafkaConsumerOffsetSet里的kafkaConsumerOffset
				for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
					if(consumeThread.consumer.equals(consumer)){
						logger.debug("consumeThread.kafkaConsumerOffsetSet remove kafkaConsumerOffset, the kafkaConsumerOffset is " + kafkaConsumerOffset);
						consumeThread.kafkaConsumerOffsetSet.remove(kafkaConsumerOffset);
					}
				}
			}
		}
		//有特殊的情况，就是两个线程同时拥有一个partition，这时，需要手动清空kafkaConsumerOffsetSet
		for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
			if(consumeThread.consumer.equals(consumer)){	
				consumeThread.kafkaConsumerOffsetSet.clear();
				if(consumeThread.assignedPartitions != null){
					consumeThread.assignedPartitions = null;
				}
			}
		}
		logger.info("finish onPartitionsRevoked!");
	}

	// onPartitionsAssigned 这个方法 group 已经进行 reassignment
	// 之后，开始拉取数据之前调用，作用是清理内存中不属于这个线程的 msg、获取 partition 的 last committed offset。
	@Override
	public void onPartitionsAssigned(
			Collection<TopicPartition> partitions) {
		logger.info("start onPartitionsAssigned!");
		Date now = new Date();
		for (TopicPartition partition : partitions) {
			// TODO 改动源码查找partition里的logsize,然后和mysql,redis里的offset进行比较，取最小值
			consumer.seek(partition,offsetManager.readOffsetFromCache(partition.topic(), partition.partition()).getOffset());
			KafkaConsumerOffset kafkaConsumerOffset = KafkaCache.searchKafkaConsumerOffset(partition.topic(), partition.partition());
			// 设定owner
			kafkaConsumerOffset
					.setOwner(KafkaMysqlOffsetParameter.kafkaClusterName
							+ "-"
							+ partition.topic()
							+ "-"
							+ partition.partition()
							+ "-"
							+ KafkaMysqlOffsetParameter.consumerGroup
							+ "-"
							+ now.getTime()
							+ "-"
							+ KafkaMysqlOffsetParameter.hostname
							+ "-"
							+ consumer.toString().substring(
									consumer.toString()
											.lastIndexOf("@") + 1));
			MysqlOffsetPersist.getInstance().updateOwner(kafkaConsumerOffset);
			for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
				if(consumeThread.consumer.equals(consumer)){
					consumeThread.assignedPartitions = partitions;
				}
			}
		}
		logger.info("finish onPartitionsAssigned!");
	}
}