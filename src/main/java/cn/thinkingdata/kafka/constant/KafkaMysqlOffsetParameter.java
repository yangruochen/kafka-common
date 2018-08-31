package cn.thinkingdata.kafka.constant;

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
	public static String maxPartitionFetchBytes = "524288";

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
	
	public static void setMaxPartitionFetchBytes(Long maxPartitionFetchBytes) {
		KafkaMysqlOffsetParameter.maxPartitionFetchBytes = maxPartitionFetchBytes.toString();
	}
	
	

}
