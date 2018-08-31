package cn.thinkingdata.kafka.common;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaConfig extends Configuration {

	private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
	
	private static KafkaConfig dwConf = null;
	
	static {
		dwConf = new KafkaConfig();
		dwConf.addResource("kafka-config.xml");
		logger.info("add resource: kafka-config.xml");
	}

	public static String getCrawlDataTopicName(){
		return dwConf.get("dw.kafka.topic.name.crawl_data");
	}
	public static String getHttpCollectorTopicName(){
		return dwConf.get("dw.kafka.topic.name.http_collector");
	}
	public static String getTestTopicName(){
		return dwConf.get("dw.kafka.topic.name.test");
	}
	public static String getQQChatTopicName(){
		return dwConf.get("dw.kafka.topic.name.qq_chat");
	}
	
}
