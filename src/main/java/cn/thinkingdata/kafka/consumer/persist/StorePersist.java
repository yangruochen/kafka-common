package cn.thinkingdata.kafka.consumer.persist;

import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.exception.ExceptionHandler;

public interface StorePersist extends ExceptionHandler {

    // 从另一个备用存储读取的接口如果读取成功，则把它删除，默认是空
    KafkaConsumerOffset readOffsetFromBackupExternalStore(String topic, int partition);

    // 写一个存到备用存储的接口，默认是空
    Boolean saveOffsetInBackupExternalStore(KafkaConsumerOffset kafkaConsumerOffset);

    Boolean backupStoreStateCheck();

    Boolean updateOwner(KafkaConsumerOffset kafkaConsumerOffset);

}
