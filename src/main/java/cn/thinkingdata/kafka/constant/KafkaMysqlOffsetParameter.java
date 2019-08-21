package cn.thinkingdata.kafka.constant;

import cn.thinkingdata.kafka.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public static void setPollInterval(Integer pollInterval) {
		KafkaMysqlOffsetParameter.pollInterval = pollInterval;
	}

	// public static void setMaxPartitionFetchBytes(Long maxPartitionFetchBytes)
	// {
	// KafkaMysqlOffsetParameter.maxPartitionFetchBytes =
	// maxPartitionFetchBytes.toString();
	// }

	public static void createKafkaConfProp() {
		kafkaConf = new Properties();
		kafkaConf
				.put("bootstrap.servers", KafkaMysqlOffsetParameter.brokerList);
		kafkaConf.put("group.id", KafkaMysqlOffsetParameter.consumerGroup);
		// Below is a key setting to turn off the auto commit.
		kafkaConf.put("enable.auto.commit", "false");
		kafkaConf.put("heartbeat.interval.ms",
				KafkaMysqlOffsetParameter.heartbeatInterval);
		kafkaConf.put("session.timeout.ms",
				KafkaMysqlOffsetParameter.sessionTimeout);
		kafkaConf.put("max.poll.records", maxPollRecords);
		kafkaConf.put("request.timeout.ms",
				KafkaMysqlOffsetParameter.requestTimeout);
		// Control maximum data on each poll, make sure this value is bigger
		// than the maximum single record size
		kafkaConf.put("max.partition.fetch.bytes",
				KafkaMysqlOffsetParameter.maxPartitionFetchBytes);
		kafkaConf.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConf.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConf.put("auto.offset.reset",
				KafkaMysqlOffsetParameter.autoOffsetReset);
	}

	public static void createKafkaConfProp(Properties prop) {
		kafkaConf = prop;
	}

	public static void createKafkaConfProp(Long maxPartitionFetchBytes,
                                           Integer heartbeatInterval, Integer sessionTimeout,
                                           Integer requestTimeout, String autoOffsetReset) {
		heartbeatInterval = heartbeatInterval * 1000;
		sessionTimeout = sessionTimeout * 1000;
		requestTimeout = requestTimeout * 1000;
		KafkaMysqlOffsetParameter.heartbeatInterval = heartbeatInterval
				.toString();
		KafkaMysqlOffsetParameter.sessionTimeout = sessionTimeout.toString();
		KafkaMysqlOffsetParameter.requestTimeout = requestTimeout.toString();
		KafkaMysqlOffsetParameter.maxPartitionFetchBytes = maxPartitionFetchBytes
				.toString();
		if (!autoOffsetReset.equals("latest")
				&& !autoOffsetReset.equals("earliest") && !autoOffsetReset.equals("none")) {
			logger.error("the parameter is not correct, the configuration auto.offset.reset: String must be one of: latest, earliest, none, your autoOffsetReset is "
					+ autoOffsetReset);
		} else {
			KafkaMysqlOffsetParameter.autoOffsetReset = autoOffsetReset;
		}

		createKafkaConfProp();
	}

}
