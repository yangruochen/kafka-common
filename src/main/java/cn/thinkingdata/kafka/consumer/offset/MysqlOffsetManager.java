package cn.thinkingdata.kafka.consumer.offset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Date;

import kafka.common.TopicAndPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;

public class MysqlOffsetManager extends OffsetManager {

	private static MysqlOffsetManager instance;

	private static final Logger logger = LoggerFactory
			.getLogger(MysqlOffsetManager.class);

	public static synchronized MysqlOffsetManager getInstance() {
		if (instance == null) {
			instance = new MysqlOffsetManager();
		}
		return instance;
	}

	Connection conn;
	String driver = "com.mysql.jdbc.Driver";

	private MysqlOffsetManager() {

		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(
					KafkaMysqlOffsetParameter.jdbcUrl,
					KafkaMysqlOffsetParameter.username,
					KafkaMysqlOffsetParameter.password);
			if (!conn.isClosed())
				System.out.println("Succeeded connecting to the Database!");
		} catch (Exception e) {
			logger.error("connect to mysql error, the error is " + e.toString());
		}

	}

	@Override
	protected synchronized void saveOffsetInExternalStore(
			KafkaConsumerOffset kafkaConsumerOffset) {
		logger.debug("because of the muti-thread, the value is not exactly right, kafkaConsumerOffset is "
				+ kafkaConsumerOffset.toString());
		try {
			Date now = new Date();
			String sql = "INSERT INTO "
					+ KafkaMysqlOffsetParameter.tableName
					+ " VALUES"
					+ " (null,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY"
					+ " UPDATE offset=?, last_flush_offset=?, kafka_cluster_name=?,"
					+ " owner=?, update_time=?;";
			PreparedStatement ps = conn.prepareStatement(sql);
			// 更新的Update_time
			kafkaConsumerOffset.setUpdate_time(now);
			if (kafkaConsumerOffset.getCreate_time() == null)
				kafkaConsumerOffset.setCreate_time(now);
			ps.setString(1, kafkaConsumerOffset.getTopic());
			ps.setInt(2, kafkaConsumerOffset.getPartition());
			ps.setString(3, kafkaConsumerOffset.getConsumer_group());
			// 得到Last_flush_offset防止consumeThread线程修改数据
			ps.setLong(5, kafkaConsumerOffset.getLast_flush_offset());
			Long last_flush_offset = kafkaConsumerOffset.getOffset();
			ps.setLong(4, kafkaConsumerOffset.getOffset());
			ps.setString(6, kafkaConsumerOffset.getKafka_cluster_name());
			ps.setString(7, kafkaConsumerOffset.getOwner());
			ps.setTimestamp(8, new Timestamp(kafkaConsumerOffset
					.getUpdate_time().getTime()));
			ps.setTimestamp(9, new Timestamp(kafkaConsumerOffset
					.getCreate_time().getTime()));
			ps.setLong(11, kafkaConsumerOffset.getLast_flush_offset());
			ps.setLong(10, kafkaConsumerOffset.getOffset());
			ps.setString(12, kafkaConsumerOffset.getKafka_cluster_name());
			ps.setString(13, kafkaConsumerOffset.getOwner());
			ps.setTimestamp(14, new Timestamp(kafkaConsumerOffset
					.getUpdate_time().getTime()));
			Boolean result = ps.execute();
			kafkaConsumerOffset.setLast_flush_offset(last_flush_offset);
			kafkaConsumerOffset.setCount(0L);
			KafkaMysqlOffsetParameter.mysqlConnState.set(true);
		} catch (SQLException e) {
			KafkaMysqlOffsetParameter.mysqlConnState.set(false);
			logger.error("mysql save offset error, the error is "
					+ e.toString());
		}
	}

	@Override
	protected KafkaConsumerOffset readOffsetFromExternalStore(String topic,
			int partition) {
		KafkaConsumerOffset kafkaConsumerOffset = new KafkaConsumerOffset();
		Statement statement = null;
		ResultSet rs = null;
		Date now = new Date();
		try {
			statement = conn.createStatement();
			String sql = "select * from " + KafkaMysqlOffsetParameter.tableName
					+ " where kafka_cluster_name = '"
					+ KafkaMysqlOffsetParameter.kafkaClusterName
					+ "' and topic = '" + topic + "' and kafka_partition = "
					+ partition + " and consumer_group = '"
					+ KafkaMysqlOffsetParameter.consumerGroup + "';";
			rs = statement.executeQuery(sql);
			KafkaMysqlOffsetParameter.mysqlConnState.set(true);
			int count = 0;
			while (rs.next()) {
				count++;
				if (count > 1) {
					logger.error("DUPLICATE KEY in "
							+ KafkaMysqlOffsetParameter.tableName
							+ " , the kafka cluster name is "
							+ KafkaMysqlOffsetParameter.kafkaClusterName
							+ " , the topic is " + topic + ", the partition is "
							+ partition + ", the consumerGroup is "
							+ KafkaMysqlOffsetParameter.consumerGroup);
					return kafkaConsumerOffset;
				}
				kafkaConsumerOffset.setOid(rs.getInt("oid"));
				kafkaConsumerOffset.setTopic(topic);
				kafkaConsumerOffset.setPartition(partition);
				kafkaConsumerOffset
						.setConsumer_group(KafkaMysqlOffsetParameter.consumerGroup);
				kafkaConsumerOffset.setOffset(rs.getLong("offset"));
				kafkaConsumerOffset.setLast_flush_offset(rs.getLong("offset"));
				kafkaConsumerOffset
						.setKafka_cluster_name(KafkaMysqlOffsetParameter.kafkaClusterName);
				kafkaConsumerOffset.setOwner(rs.getString("owner"));
				kafkaConsumerOffset.setCount(0L);
				kafkaConsumerOffset.setUpdate_time(rs.getDate("update_time"));
				kafkaConsumerOffset.setCreate_time(rs.getDate("create_time"));
			}
			if (count == 0) {
				logger.info("offset is not in "
						+ KafkaMysqlOffsetParameter.tableName
						+ " , the kafka cluster name is "
						+ KafkaMysqlOffsetParameter.kafkaClusterName
						+ " , the topic is " + topic + ", the partition is "
						+ partition + ", the consumerGroup is "
						+ KafkaMysqlOffsetParameter.consumerGroup);
				kafkaConsumerOffset.setTopic(topic);
				kafkaConsumerOffset.setPartition(partition);
				kafkaConsumerOffset
						.setConsumer_group(KafkaMysqlOffsetParameter.consumerGroup);
				kafkaConsumerOffset.setOffset(0L);
				kafkaConsumerOffset.setLast_flush_offset(0L);
				kafkaConsumerOffset
						.setKafka_cluster_name(KafkaMysqlOffsetParameter.kafkaClusterName);
				kafkaConsumerOffset.setCount(0L);
				kafkaConsumerOffset.setUpdate_time(now);
				return kafkaConsumerOffset;
			}
		} catch (Exception e) {
			KafkaMysqlOffsetParameter.mysqlConnState.set(false);
			logger.error("mysql read offset error, the error is "
					+ e.toString());
			return null;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				statement.close();
			} catch (SQLException e) {
				logger.error("can not close the ResultSet or the Statement of mysql, the error is "
						+ e.toString());
			}
		}
		return kafkaConsumerOffset;
	}

	public void saveAllOffsetInCacheToMysql() {
		for (KafkaConsumerOffset kafkaConsumerOffset : KafkaCache.kafkaConsumerOffsets) {
			saveOffsetInCacheToMysql(kafkaConsumerOffset);
		}
	}

	public void shutdown() {
		logger.info("mysql shutdown!");
		try {
			conn.close();
		} catch (SQLException e) {
			logger.error("can not close connection of mysql, the error is "
					+ e.toString());
		}
	}

	public synchronized void saveOffsetInCacheToMysql(
			KafkaConsumerOffset kafkaConsumerOffset) {
		Long lag = kafkaConsumerOffset.getOffset()
				- kafkaConsumerOffset.getLast_flush_offset();
		if (!lag.equals(0L)) {
			logger.debug("because of the muti-thread, the value is not exactly right, the lag is "
					+ lag);
			saveOffsetInExternalStore(kafkaConsumerOffset);
		}
	}

	public void mysqlStateCheck() {
		Statement statement = null;
		ResultSet rs = null;
		String sql = "select * from " + KafkaMysqlOffsetParameter.tableName
				+ " limit 10;";
		try {
			statement = conn.createStatement();
			rs = statement.executeQuery(sql);
			KafkaMysqlOffsetParameter.mysqlConnState.set(true);
			logger.info("mysql reconnected!");
		} catch (SQLException e) {
			KafkaMysqlOffsetParameter.mysqlConnState.set(false);
			// logger.error("can not connect to mysql, the error is "
			// + e.toString());
		}
	}

	public void deleteOwner(KafkaConsumerOffset kafkaConsumerOffset) {
		logger.debug("delete the owner for kafkaConsumerOffset, kafkaConsumerOffset is "
				+ kafkaConsumerOffset.toString());
		Date now = new Date();
		Statement statement = null;
		Boolean rs = null;
		try {
			statement = conn.createStatement();
			String sql = "UPDATE " + KafkaMysqlOffsetParameter.tableName
					+ " set owner='', update_time = NOW()"
					+ " where kafka_cluster_name = '"
					+ KafkaMysqlOffsetParameter.kafkaClusterName
					+ "' and topic = '" + kafkaConsumerOffset.getTopic()
					+ "' and kafka_partition = "
					+ kafkaConsumerOffset.getPartition()
					+ " and consumer_group = '"
					+ KafkaMysqlOffsetParameter.consumerGroup + "';";
			rs = statement.execute(sql);
			KafkaMysqlOffsetParameter.mysqlConnState.set(true);
		} catch (SQLException e) {
			KafkaMysqlOffsetParameter.mysqlConnState.set(false);
			logger.error("mysql save offset error, the error is "
					+ e.toString());
		}
	}
}
