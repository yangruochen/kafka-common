package cn.thinkingdata.kafka.cache;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.KafkaConsumerRebalancerListener;
import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumeThread;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;

public class KafkaCache {

	public static Set<KafkaConsumerOffset> kafkaConsumerOffsets = ConcurrentHashMap
			.newKeySet();
	public static List<KafkaSubscribeConsumeThread> consumeThreadList = new CopyOnWriteArrayList<KafkaSubscribeConsumeThread>();
	public static List<KafkaConsumerRebalancerListener> rebalancerListenerList = new CopyOnWriteArrayList<KafkaConsumerRebalancerListener>();

	public static KafkaConsumerOffset searchKafkaConsumerOffset(String topic,
			int partition) {
		KafkaConsumerOffset kafkaConsumerOffset = new KafkaConsumerOffset();
		kafkaConsumerOffset
				.setKafka_cluster_name(KafkaMysqlOffsetParameter.kafkaClusterName);
		kafkaConsumerOffset.setTopic(topic);
		kafkaConsumerOffset.setPartition(partition);
		kafkaConsumerOffset
				.setConsumer_group(KafkaMysqlOffsetParameter.consumerGroup);
		for (KafkaConsumerOffset kafkaConsumerOffsetInCache : KafkaCache.kafkaConsumerOffsets) {
			if (kafkaConsumerOffset.equals(kafkaConsumerOffsetInCache)) {
				kafkaConsumerOffset = kafkaConsumerOffsetInCache;
				return kafkaConsumerOffset;
			}
		}
		return null;
	}

}
