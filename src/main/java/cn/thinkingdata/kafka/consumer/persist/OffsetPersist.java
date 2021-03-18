package cn.thinkingdata.kafka.consumer.persist;

import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;

public interface OffsetPersist {

    void persist(KafkaConsumerOffset kafkaConsumerOffsetInCache);

    void shutdown();

    Boolean flush(KafkaConsumerOffset kafkaConsumerOffsetInCache);
}
