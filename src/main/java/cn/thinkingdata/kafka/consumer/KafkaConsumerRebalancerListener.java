package cn.thinkingdata.kafka.consumer;

import java.util.Collection;
import java.util.Date;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.offset.MysqlOffsetManager;
import cn.thinkingdata.kafka.consumer.offset.OffsetManager;
import cn.thinkingdata.kafka.consumer.persist.MysqlOffsetPersist;

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
	public void onPartitionsRevoked(
			Collection<TopicPartition> partitions) {
		logger.info("start onPartitionsRevoked!");
		// 等待所有consumer线程写入consumer动作完成
		for (;;) {
			Boolean consumerDataRunFlag = false;
			for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
				if (consumeThread != null && consumeThread.consumerDataRunFlag)
					consumerDataRunFlag = true;
			}
			if (!consumerDataRunFlag)
				break;
		}
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
						consumeThread.kafkaConsumerOffsetSet.remove(kafkaConsumerOffset);
					}
				}
			}
		}
		logger.info("finish onPartitionsRevoked!");
	}

	// onPartitionsAssigned 这个方法 group 已经进行 reassignment
	// 之后，开始拉取数据之前调用，作用是清理内存中不属于这个线程的 msg、获取 partition 的 last committed offset。
	public void onPartitionsAssigned(
			Collection<TopicPartition> partitions) {
		logger.info("start onPartitionsAssigned!");
		Date now = new Date();
		for (TopicPartition partition : partitions) {
			consumer.seek(
					partition,
					offsetManager.readOffsetFromCache(partition.topic(),
							partition.partition()).getOffset());
			KafkaConsumerOffset kafkaConsumerOffset = KafkaCache
					.searchKafkaConsumerOffset(partition.topic(),
							partition.partition());
			// 设定owner
			kafkaConsumerOffset
					.setOwner(KafkaMysqlOffsetParameter.kafkaClusterName
							+ "_"
							+ partition.topic()
							+ "_"
							+ partition.partition()
							+ "_"
							+ KafkaMysqlOffsetParameter.consumerGroup
							+ "_"
							+ now.getTime()
							+ "_"
							+ KafkaMysqlOffsetParameter.hostname
							+ "_"
							+ consumer.toString().substring(
									consumer.toString()
											.lastIndexOf("@") + 1));
		}
		logger.info("finish onPartitionsAssigned!");
	}
}