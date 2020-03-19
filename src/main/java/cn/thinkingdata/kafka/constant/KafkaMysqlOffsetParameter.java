package cn.thinkingdata.kafka.constant;

import cn.thinkingdata.kafka.util.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaMysqlOffsetParameter {

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaMysqlOffsetParameter.class);

	public static final AtomicBoolean kafkaSubscribeConsumerClosed = new AtomicBoolean(
			true);
	public static final AtomicBoolean mysqlAndBackupStoreConnState = new AtomicBoolean(true);
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
	private static String heartbeatInterval = "10000";
	public static String sessionTimeout = "30000";
	private static String requestTimeout = "40000";
	public static String autoOffsetReset = "latest";
	public static Integer pollInterval = 50;
	public static Integer maxPollRecords = 1000;
	public static String partitionAssignmentStrategy;
	public static Properties kafkaConf;

	// public static void setMaxPartitionFetchBytes(Long maxPartitionFetchBytes)
	// {
	// KafkaMysqlOffsetParameter.maxPartitionFetchBytes =
	// maxPartitionFetchBytes.toString();
	// }

	public static void createKafkaConfProp() {
		kafkaConf = new Properties();
		kafkaConf.put("bootstrap.servers", KafkaMysqlOffsetParameter.brokerList);
		kafkaConf.put("group.id", KafkaMysqlOffsetParameter.consumerGroup);
		// Below is a key setting to turn off the auto commit.
		kafkaConf.put("enable.auto.commit", "false");
		kafkaConf.put("heartbeat.interval.ms", KafkaMysqlOffsetParameter.heartbeatInterval);
		kafkaConf.put("session.timeout.ms", KafkaMysqlOffsetParameter.sessionTimeout);
		kafkaConf.put("max.poll.records", maxPollRecords);
		kafkaConf.put("request.timeout.ms", KafkaMysqlOffsetParameter.requestTimeout);
		// Control maximum data on each poll, make sure this value is bigger
		// than the maximum single record size
		kafkaConf.put("max.partition.fetch.bytes", KafkaMysqlOffsetParameter.maxPartitionFetchBytes);
		kafkaConf.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConf.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConf.put("auto.offset.reset", KafkaMysqlOffsetParameter.autoOffsetReset);
		if(StringUtils.isNotBlank(partitionAssignmentStrategy)){
			kafkaConf.put("partition.assignment.strategy", KafkaMysqlOffsetParameter.partitionAssignmentStrategy);
		}
	}

	public static void createKafkaConfProp(Map<String, String> prop) {

		jdbcUrl = prop.get("jdbc.url");
		username = prop.get("username");
		password = prop.get("password");
		tableName = prop.get("table.name");
		brokerList = prop.get("broker.list");
		kafkaClusterName = prop.get("kafka.cluster.name");
		topic = prop.get("topic");
		consumerGroup = prop.get("consumer.group");
		processThreadNum = Integer.parseInt(prop.get("process.thread.num"));
		flushOffsetSize = Integer.parseInt(prop.get("flush.offset.size"));
		flushInterval = Integer.parseInt(prop.get("flush.interval"));

		assert null != jdbcUrl;
		assert null != username;
		assert null != password;
		assert null != tableName;
		assert null != brokerList;
		assert null != kafkaClusterName;
		assert null != topic;
		assert null != consumerGroup;
		assert null != processThreadNum;
		assert null != flushOffsetSize;
		assert null != flushInterval;


		if(prop.get("heartbeat.interval") != null){
			heartbeatInterval = String.valueOf(Integer.parseInt(prop.get("heartbeat.interval")) * 1000);
		}
		if(prop.get("session.timeout") != null){
			sessionTimeout = String.valueOf(Integer.parseInt(prop.get("session.timeout")) * 1000);
		}
		if(prop.get("request.timeout") != null){
			requestTimeout = String.valueOf(Integer.parseInt(prop.get("request.timeout")) * 1000);
		}
		if(prop.get("max.partition.fetch.bytes") != null){
			maxPartitionFetchBytes = prop.get("max.partition.fetch.bytes");
		}
		if(prop.get("auto.offset.reset") != null) {
			autoOffsetReset = prop.get("auto.offset.reset");
			assert (autoOffsetReset.equals("latest") || autoOffsetReset.equals("earliest") || autoOffsetReset.equals("none"));
		}
		if(prop.get("poll.interval") != null){
			pollInterval = Integer.parseInt(prop.get("poll.interval"));
		}
		if(prop.get("max.poll.records") != null){
			maxPollRecords = Integer.parseInt(prop.get("max.poll.records"));
		}
		if(prop.get("partition.assignment.strategy") != null){
			partitionAssignmentStrategy = prop.get("partition.assignment.strategy");
		}
		createKafkaConfProp();
	}

}
