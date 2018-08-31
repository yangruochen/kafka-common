package cn.thinkingdata.kafka.consumer.offset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import kafka.common.TopicAndPartition;

public abstract class OffsetManager {
	
	private static final Logger logger = LoggerFactory
			.getLogger(OffsetManager.class);
	
	public void saveOffsetInCache(KafkaConsumerOffset kafkaConsumerOffset) {

		KafkaConsumerOffset kafkaConsumerOffsetOld = KafkaCache.searchKafkaConsumerOffset(kafkaConsumerOffset.getTopic(), kafkaConsumerOffset.getPartition());
		if(kafkaConsumerOffsetOld == null || !kafkaConsumerOffset.getCount().equals(0L)){
			KafkaCache.kafkaConsumerOffsets.add(kafkaConsumerOffset);
			kafkaConsumerOffset.setCount(0L);
		}
	}

	public KafkaConsumerOffset readOffsetFromCache(String topic, int partition) {
		KafkaConsumerOffset kafkaConsumerOffset = KafkaCache
				.searchKafkaConsumerOffset(topic, partition);
		if (kafkaConsumerOffset == null) {
			kafkaConsumerOffset = readOffsetFromExternalStore(topic, partition);
			if (kafkaConsumerOffset != null) {
				KafkaCache.kafkaConsumerOffsets.add(kafkaConsumerOffset);
			} else {
				logger.error("the kafkaConsumerOffset read from external store is null , the topic is " + topic + "the partition is " + partition);
			}
		}
		return kafkaConsumerOffset;
	}

	abstract void saveOffsetInExternalStore(
			KafkaConsumerOffset kafkaConsumerOffset);

	abstract KafkaConsumerOffset readOffsetFromExternalStore(String topic,
			int partition);
}
