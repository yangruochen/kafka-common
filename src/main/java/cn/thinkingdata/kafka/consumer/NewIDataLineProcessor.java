package cn.thinkingdata.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface NewIDataLineProcessor {

    void processData(ConsumerRecord<String, String> consumerRecord);

    void finishProcess();
}
