package cn.thinkingdata.kafka.consumer.offset;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumeThread;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.persist.DefaultStorePersist;
import cn.thinkingdata.kafka.consumer.persist.StorePersist;
import cn.thinkingdata.kafka.util.CommonUtils;
import cn.thinkingdata.kafka.util.RetryerUtil;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.common.base.Predicates;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public abstract class OffsetManager {

    private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);

    private final Retryer<KafkaConsumerOffset> retryerWithResultNull = RetryerUtil.initRetryerByTimesWithIfResult(3, 300, Predicates.isNull());

    private StorePersist externalStorePersist = new DefaultStorePersist();

    public StorePersist getExternalStorePersist() {
        return externalStorePersist;
    }

    public void setExternalStorePersist(StorePersist externalStorePersist) {
        this.externalStorePersist = externalStorePersist;
    }

    public void saveOffsetInCache(KafkaSubscribeConsumeThread consumeThread, KafkaConsumerOffset kafkaConsumerOffset) {
        TopicPartition topicPartition = new TopicPartition(kafkaConsumerOffset.getTopic(), kafkaConsumerOffset.getPartition());
        KafkaConsumerOffset kafkaConsumerOffsetOld = KafkaCache.kafkaConsumerOffsetMaps.get(topicPartition);
        // compare kafkaConsumerOffsetOld and kafkaConsumerOffset, avoid reset
        if (kafkaConsumerOffsetOld != null && kafkaConsumerOffsetOld.getOffset() > kafkaConsumerOffset.getOffset()) {
            logger.info("kafka consumer offset reset, the old kafkaConsumerOffset is " + kafkaConsumerOffsetOld + ", the kafkaConsumerOffset is " + kafkaConsumerOffset);
            synchronized (OffsetManager.class) {
                externalStorePersist.executeWhenOffsetReset(consumeThread);
            }
        } else if (kafkaConsumerOffsetOld == null
                || !kafkaConsumerOffset.getCount().equals(0L)) {
            KafkaCache.kafkaConsumerOffsetMaps.put(topicPartition, kafkaConsumerOffset);
            kafkaConsumerOffset.setCount(0L);
        }
    }

    public KafkaConsumerOffset readOffsetFromMysql(final String topic, final Integer partition) {
        KafkaConsumerOffset kafkaConsumerOffset = null;
        try {
            kafkaConsumerOffset = retryerWithResultNull
                    .call(() -> readOffsetFromExternalStore(topic, partition));
            if (kafkaConsumerOffset == null) {
                logger.error("the kafkaConsumerOffset read from mysql is null , the topic is " + topic + "the partition is " + partition);
            }
        } catch (ExecutionException | RetryException e) {
            logger.error("retry to read kafkaConsumerOffset from mysql error, the error is " + CommonUtils.getStackTraceAsString(e));
            return null;
        }
        return kafkaConsumerOffset;
    }

    public KafkaConsumerOffset readOffsetFromBackupExternalStore(
            final String topic, final Integer partition) {
        KafkaConsumerOffset kafkaConsumerOffset = null;
        try {
            kafkaConsumerOffset = retryerWithResultNull
                    .call(() -> externalStorePersist.readOffsetFromBackupExternalStore(topic, partition));
            if (kafkaConsumerOffset == null) {
                logger.error("the kafkaConsumerOffset read from backup external store is null , the topic is " + topic + "the partition is " + partition);
            }
        } catch (ExecutionException | RetryException e) {
            logger.error("retry to read kafkaConsumerOffset from backup external store error, the error is " + CommonUtils.getStackTraceAsString(e));
            return null;
        }
        return kafkaConsumerOffset;
    }

    public synchronized KafkaConsumerOffset readOffsetFromCache(String topic, Integer partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        KafkaConsumerOffset kafkaConsumerOffset = KafkaCache.kafkaConsumerOffsetMaps.get(topicPartition);
        if (kafkaConsumerOffset == null) {
            kafkaConsumerOffset = readOffsetFromMysql(topic, partition);
            if (kafkaConsumerOffset == null) {
                logger.error("can not read offset from mysql! the topic is " + topic + ",the partition is " + partition);
                kafkaConsumerOffset = externalStorePersist.executeWhenReadNullFromMysql(topic, partition);
            }
            // 从另一个备用存储读取的接口如果读取成功，默认是空
            KafkaConsumerOffset kafkaConsumerOffsetFromBackupExternalStore = readOffsetFromBackupExternalStore(topic, partition);
            if (kafkaConsumerOffsetFromBackupExternalStore == null) {
                logger.error("can not read offset from backup external store! the topic is " + topic + ",the partition is " + partition);
                kafkaConsumerOffsetFromBackupExternalStore = externalStorePersist.executeWhenReadNullFromBackupExternalStore(topic, partition);
            }
            // 判断两个存储中的数值，然后确定用offset更大的那个
            kafkaConsumerOffset = getKafkaConsumerOffsetFromMysqlAndBackupExternalStore(kafkaConsumerOffset, kafkaConsumerOffsetFromBackupExternalStore);
            if (kafkaConsumerOffset != null) {
                KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState.set(true);
                KafkaCache.kafkaConsumerOffsetMaps.put(topicPartition, kafkaConsumerOffset);
            } else {
                KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState.set(false);
                logger.error("the kafkaConsumerOffset read from external store is null , the topic is " + topic + ",the partition is " + partition);
            }
        }
        return kafkaConsumerOffset;
    }

    public KafkaConsumerOffset getKafkaConsumerOffsetFromMysqlAndBackupExternalStore(
            KafkaConsumerOffset kafkaConsumerOffset,
            KafkaConsumerOffset kafkaConsumerOffsetFromBackupExternalStore) {
        if (kafkaConsumerOffsetFromBackupExternalStore == null) {
            logger.error("getKafkaConsumerOffsetFromMysqlAndBackupExternalStore, the kafka consumer offset from backup external store is null!");
            System.exit(-1);
            return kafkaConsumerOffset;
        }
        if (kafkaConsumerOffsetFromBackupExternalStore.isNull()) {
            logger.info("getKafkaConsumerOffsetFromMysqlAndBackupExternalStore, the kafka consumer offset from backup external store is null, the offset is "
                    + kafkaConsumerOffsetFromBackupExternalStore);
            return kafkaConsumerOffset;
        }
        if (!kafkaConsumerOffsetFromBackupExternalStore
                .equals(kafkaConsumerOffset)) {
            logger.error("getKafkaConsumerOffsetFromMysqlAndBackupExternalStore error, the kafkaConsumerOffsetFromBackupExternalStore is "
                    + kafkaConsumerOffsetFromBackupExternalStore
                    + ", the kafkaConsumerOffset is "
                    + kafkaConsumerOffset
                    + ", they should be equal!");
            System.exit(-1);
            return kafkaConsumerOffset;
        }
        if (kafkaConsumerOffsetFromBackupExternalStore.getOffset() > kafkaConsumerOffset.getOffset()) {
            kafkaConsumerOffset = kafkaConsumerOffsetFromBackupExternalStore;
        }
        return kafkaConsumerOffset;
    }

    abstract Boolean saveOffsetInExternalStore(KafkaConsumerOffset kafkaConsumerOffset);

    abstract KafkaConsumerOffset readOffsetFromExternalStore(String topic, int partition);

}
