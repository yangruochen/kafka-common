package cn.thinkingdata.kafka.cache;

import cn.thinkingdata.kafka.consumer.KafkaConsumerRebalancerListener;
import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumeThread;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class KafkaCache {

    public static Map<TopicPartition, KafkaConsumerOffset> kafkaConsumerOffsetMaps = new ConcurrentHashMap();
    public static List<KafkaSubscribeConsumeThread> consumeThreadList = new CopyOnWriteArrayList<KafkaSubscribeConsumeThread>();
    public static List<KafkaConsumerRebalancerListener> rebalancerListenerList = new CopyOnWriteArrayList<KafkaConsumerRebalancerListener>();

}
