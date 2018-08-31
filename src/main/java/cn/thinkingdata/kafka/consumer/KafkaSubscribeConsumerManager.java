package cn.thinkingdata.kafka.consumer;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import cn.thinkingdata.kafka.cache.KafkaCache;

public class KafkaSubscribeConsumerManager {
	
	private static KafkaSubscribeConsumerManager instance;

	private Logger logger = Logger.getLogger(KafkaSubscribeConsumerManager.class);

	private KafkaSubscribeConsumerManager() {
	}

	public static synchronized KafkaSubscribeConsumerManager getInstance() {
		if (instance == null) {
			instance = new KafkaSubscribeConsumerManager();
		}
		return instance;
	}

	public KafkaConsumer<String, String> createKafkaConsumer(List<String> topicList, Properties props){
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		KafkaConsumerRebalancerListener rebalancerListener = new KafkaConsumerRebalancerListener(consumer);
		KafkaCache.rebalancerListenerList.add(rebalancerListener);
		consumer.subscribe(topicList, rebalancerListener);
		return consumer;
	}
	
	public void destoryKafkaConsumer(KafkaConsumer<String, String> consumer){
		consumer.close();
	}
}