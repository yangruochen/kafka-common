package cn.thinkingdata.kafka.consumer.exception;

import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumeThread;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;

public interface ExceptionHandler {
	
	KafkaConsumerOffset executeWhenReadNullFromBackupExternalStore(String topic, Integer partition);

	KafkaConsumerOffset executeWhenReadNullFromMysql(String topic, Integer partition);
	
	Boolean executeWhenSaveOffsetFailInMysqlAndExternalStore(KafkaConsumerOffset kafkaConsumerOffset);
	
	void executeWhenSessionTimeout(Integer count);
	
	void executeWhenExecuteDataSessionTimeout(KafkaSubscribeConsumeThread kafkaSubscribeConsumeThread);

	void executeWhenOffsetReset(KafkaSubscribeConsumeThread consumeThread);

	void executeWhenException();

}
