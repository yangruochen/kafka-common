package cn.thinkingdata.kafka.consumer.persist;

import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumeThread;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.exception.TaKafkaCommonException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStorePersist implements StorePersist {

	private static final Logger logger = LoggerFactory
			.getLogger(DefaultStorePersist.class);

	@Override
	public KafkaConsumerOffset readOffsetFromBackupExternalStore(String topic,
                                                                 int partition) {
		return new KafkaConsumerOffset();
	}

	// @Override
	// public Boolean clearOffsetFromBackupExternalStore(String topic,
	// int partition) {
	// return true;
	// }

	@Override
	public Boolean saveOffsetInBackupExternalStore(
			KafkaConsumerOffset kafkaConsumerOffset) {
		return true;
	}

	@Override
	public KafkaConsumerOffset executeWhenReadNullFromBackupExternalStore(
            String topic, Integer partition) {
		logger.error("can not read offset from backup external store!");
//		System.exit(-1);
//		return null;
		throw new TaKafkaCommonException("executeWhenReadNullFromBackupExternalStore, can not read offset from backup external store!");

	}

	@Override
	public KafkaConsumerOffset executeWhenReadNullFromMysql(String topic,
                                                            Integer partition) {
		logger.error("can not read offset from mysql!");
//		System.exit(-1);
//		return null;
		throw new TaKafkaCommonException("executeWhenReadNullFromMysql, can not read offset from mysql!");
	}

	// @Override
	// public Boolean executeWhenNotClearExternalStore(String topic, Integer
	// partition) {
	// return true;
	// }

	@Override
	public Boolean executeWhenSaveOffsetFailInMysqlAndExternalStore(
			KafkaConsumerOffset kafkaConsumerOffset) {
		logger.error("save offset fail in mysql or external store!");
//		System.exit(-1);
//		return null;
		throw new TaKafkaCommonException("executeWhenSaveOffsetFailInMysqlAndExternalStore, save offset fail in mysql or external store!");
	}

	@Override
	public Boolean backupStoreStateCheck() {
		return true;
	}

	@Override
	public Boolean updateOwner(KafkaConsumerOffset kafkaConsumerOffset) {
		return true;
	}

	@Override
	public void executeWhenSessionTimeout(Integer count) {
		logger.info("session will time out! the count is " + count
				+ ", the session time out is "
				+ KafkaMysqlOffsetParameter.sessionTimeout);
//		System.exit(-1);
		throw new TaKafkaCommonException("executeWhenSessionTimeout, session will time out! the count is " + count
				+ ", the session time out is "
				+ KafkaMysqlOffsetParameter.sessionTimeout);
	}

	@Override
	public void executeWhenExecuteDataSessionTimeout(KafkaSubscribeConsumeThread kafkaSubscribeConsumeThread) {
		logger.info("session time out! the count is, the session time out is "
				+ KafkaMysqlOffsetParameter.sessionTimeout);
//		System.exit(-1);
		throw new TaKafkaCommonException("executeWhenExecuteDataSessionTimeout, session time out! the count is, the session time out is "
				+ KafkaMysqlOffsetParameter.sessionTimeout);
	}

	@Override
	public void executeWhenOffsetReset(KafkaSubscribeConsumeThread consumeThread) {
		logger.info("offset reset!");
//        System.exit(-1);
		throw new TaKafkaCommonException("executeWhenOffsetReset, kafka offset reset!");
	}

	@Override
	public void executeWhenException() {
		logger.info("exception!");
//		System.exit(-1);
		throw new TaKafkaCommonException("executeWhenException!");
	}

}
