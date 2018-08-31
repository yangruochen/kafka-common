package cn.thinkingdata.kafka.consumer.persist;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.offset.MysqlOffsetManager;

public class MysqlOffsetPersist extends Thread implements OffsetPersist {

	private static final Logger logger = LoggerFactory
			.getLogger(MysqlOffsetPersist.class);

	private static MysqlOffsetPersist instance;
	private static Boolean flag = true;
	public static volatile Boolean runFlag = false;
//	public static Boolean mysqlOffsetPersistFlag = false;

	public static synchronized MysqlOffsetPersist getInstance() {
		if (instance == null) {
			instance = new MysqlOffsetPersist();
		}
		return instance;
	}

	private MysqlOffsetPersist() {
	}

	// persist和flush互斥
	@Override
	public void persist(KafkaConsumerOffset kafkaConsumerOffset) {
		MysqlOffsetManager.getInstance().saveOffsetInCacheToMysql(
				kafkaConsumerOffset);
	}

	@Override
	public void shutdown() {
		// 关闭定时线程
		logger.info("------- Shutting mysql offset thread Down ---------------------");
		MysqlOffsetManager.getInstance().shutdown();
	}

	@Override
	public synchronized void flush() {
		logger.info("------- flush all offset in cache to mysql ---------------------");
		MysqlOffsetManager.getInstance().saveAllOffsetInCacheToMysql();
		KafkaCache.kafkaConsumerOffsets.clear();
	}

	public void mysqlStateCheckJob() {
		// 如果mysql连接不通，看看有没有恢复
		int count = 0;
		while (!KafkaMysqlOffsetParameter.mysqlConnState.get()) {
			if (count == 0) {
				logger.info("------- mysql down, check mysql status ---------------------");
				count++;
			}
			MysqlOffsetManager.getInstance().mysqlStateCheck();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("------- thread can not sleep ---------------------"
						+ e.toString());
			}
		}
	}

	@Override
	public void run() {
		while (flag && !KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed.get()) {
			runFlag = true;
			mysqlStateCheckJob();
			// 如果通，并且没有进行rebalance，则定时刷数据进mysql
			if (KafkaMysqlOffsetParameter.mysqlConnState.get()) {
				persisit();
			}
			try {
				Thread.sleep(new Long(KafkaMysqlOffsetParameter.flushInterval) * 100);
			} catch (InterruptedException e) {
				logger.error("------- thread can not sleep ---------------------"
						+ e.toString());
			}
		}
		runFlag = false;
		logger.info("mysql persist stop, runFlag is " + runFlag);
	}

	private synchronized void persisit() {
		Date now = new Date();
		for (KafkaConsumerOffset kafkaConsumerOffsetInCache : KafkaCache.kafkaConsumerOffsets) {
			// 根据同步offset的size，同步offset的时间
			Long lag = kafkaConsumerOffsetInCache.getOffset()
					- kafkaConsumerOffsetInCache.getLast_flush_offset();
			Long updateInterval = now.getTime()
					- kafkaConsumerOffsetInCache.getUpdate_time().getTime();
			if (lag >= KafkaMysqlOffsetParameter.flushOffsetSize
					|| updateInterval >= new Long(
							KafkaMysqlOffsetParameter.flushInterval) * 1000) {
				persist(kafkaConsumerOffsetInCache);
			}
		}
	}

	public synchronized void flush(KafkaConsumerOffset kafkaConsumerOffset) {
		logger.info("------- flush offset in cache to mysql ---------------------");
		MysqlOffsetManager.getInstance().saveOffsetInCacheToMysql(
				kafkaConsumerOffset);
		KafkaCache.kafkaConsumerOffsets.remove(kafkaConsumerOffset);

	}
}
