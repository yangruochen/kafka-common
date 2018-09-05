package cn.thinkingdata.kafka.constant;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import cn.thinkingdata.kafka.util.CommonUtils;

public class KafkaMysqlOffsetParameter {

	public static final AtomicBoolean kafkaSubscribeConsumerClosed = new AtomicBoolean(
			true);
	public static final AtomicBoolean mysqlConnState = new AtomicBoolean(true);
	public static final String hostname = CommonUtils.getHostName();
	
	public static String jdbcUrl;
	public static String username;
	public static String password;
	public static String tableName;
	public static String brokerList;
	public static String kafkaClusterName;
	public static String topic;
	public static String consumerGroup;
	public static Integer processThreadNum;
	public static Integer flushOffsetSize;
	public static Integer flushInterval;
	private static String maxPartitionFetchBytes = "524288";
	private static String heartbeatInterval = "5000";
	private static String sessionTimeout = "15001";
	public static Integer pollInterval = 1;
	public static Properties kafkaConf;
	

	public static void setValue(String jdbcUrl, String username,
			String password, String tableName, String brokerList,
			String kafkaClusterName, String topic, String consumerGroup,
			Integer processThreadNum, Integer flushOffsetSize,
			Integer flushInterval) {
		KafkaMysqlOffsetParameter.jdbcUrl = jdbcUrl;
		KafkaMysqlOffsetParameter.username = username;
		KafkaMysqlOffsetParameter.password = password;
		KafkaMysqlOffsetParameter.tableName = tableName;
		KafkaMysqlOffsetParameter.brokerList = brokerList;
		KafkaMysqlOffsetParameter.kafkaClusterName = kafkaClusterName;
		KafkaMysqlOffsetParameter.topic = topic;
		KafkaMysqlOffsetParameter.consumerGroup = consumerGroup;
		KafkaMysqlOffsetParameter.processThreadNum = processThreadNum;
		KafkaMysqlOffsetParameter.flushOffsetSize = flushOffsetSize;
		KafkaMysqlOffsetParameter.flushInterval = flushInterval;
	}
	
	public static void setPollInterval(Integer pollInterval){
		KafkaMysqlOffsetParameter.pollInterval = pollInterval;
	}
	
//	public static void setMaxPartitionFetchBytes(Long maxPartitionFetchBytes) {
//		KafkaMysqlOffsetParameter.maxPartitionFetchBytes = maxPartitionFetchBytes.toString();
//	}
	
	public static void createKafkaConfProp(){
		kafkaConf = new Properties();
		kafkaConf.put("bootstrap.servers", KafkaMysqlOffsetParameter.brokerList);
		kafkaConf.put("group.id", KafkaMysqlOffsetParameter.consumerGroup);
		// Below is a key setting to turn off the auto commit.
		kafkaConf.put("enable.auto.commit", "false");
		kafkaConf.put("heartbeat.interval.ms", KafkaMysqlOffsetParameter.heartbeatInterval);
		kafkaConf.put("session.timeout.ms", KafkaMysqlOffsetParameter.sessionTimeout);
		// Control maximum data on each poll, make sure this value is bigger
		// than the maximum single record size
		kafkaConf.put("max.partition.fetch.bytes",
				KafkaMysqlOffsetParameter.maxPartitionFetchBytes);
		kafkaConf.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConf.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
	}
	
	public static void createKafkaConfProp(Properties prop){
		kafkaConf = prop;
	}
	
	public static void createKafkaConfProp(Long maxPartitionFetchBytes, Integer heartbeatInterval, Integer sessionTimeout){
		heartbeatInterval = heartbeatInterval * 1000;
		sessionTimeout = sessionTimeout * 1000 + 1;
		KafkaMysqlOffsetParameter.heartbeatInterval=heartbeatInterval.toString();
		KafkaMysqlOffsetParameter.sessionTimeout=sessionTimeout.toString();
		KafkaMysqlOffsetParameter.maxPartitionFetchBytes=maxPartitionFetchBytes.toString();
		createKafkaConfProp();
	}
	

}
