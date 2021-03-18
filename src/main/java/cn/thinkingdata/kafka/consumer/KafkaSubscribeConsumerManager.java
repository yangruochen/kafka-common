package cn.thinkingdata.kafka.consumer;

import cn.thinkingdata.kafka.cache.KafkaCache;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

public class KafkaSubscribeConsumerManager {

    private static KafkaSubscribeConsumerManager instance;

    private KafkaSubscribeConsumerManager() {
    }

    public static synchronized KafkaSubscribeConsumerManager getInstance() {
        if (instance == null) {
            instance = new KafkaSubscribeConsumerManager();
        }
        return instance;
    }

    public KafkaConsumer<String, String> createKafkaConsumer(List<String> topicList, Properties props) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        KafkaConsumerRebalancerListener rebalancerListener = new KafkaConsumerRebalancerListener(consumer);
        KafkaCache.rebalancerListenerList.add(rebalancerListener);
        consumer.subscribe(topicList, rebalancerListener);
        return consumer;
    }
}