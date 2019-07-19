package cn.thinkingdata.kafka.consumer.persist;

public class RedisStorePersistService{

}
//import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
//import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumeThread;
//import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
//import cn.thinkingdata.kafka.consumer.persist.MysqlOffsetPersist;
//import cn.thinkingdata.kafka.consumer.persist.StorePersist;
//import cn.thinkingdata.kafka.util.CommonUtils;
//import cn.thinkingdata.kafka.util.RetryerUtil;
//import cn.thinkingdata.ta.etl.domain.Do_ta_company_info;
//import cn.thinkingdata.ta.etl.mapper.MapperTaMysql;
//import cn.thinkingdata.ta.etl.service.TaCenteredAlertSendService;
//import com.github.rholder.retry.RetryException;
//import com.github.rholder.retry.Retryer;
//import com.google.common.base.Predicates;
//import com.google.common.base.Throwables;
//import org.apache.commons.collections.MapUtils;
//import org.apache.commons.lang.StringUtils;
//import org.joda.time.DateTime;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.data.redis.connection.RedisConnection;
//import org.springframework.data.redis.core.RedisCallback;
//import org.springframework.data.redis.core.RedisTemplate;
//import org.springframework.data.redis.serializer.RedisSerializer;
//import org.springframework.stereotype.Service;
//
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ExecutionException;
//
//@Service
//public class RedisStorePersistService implements StorePersist {
//
//    private static final Logger logger = LoggerFactory
//            .getLogger(RedisStorePersistService.class);
//
//    public static volatile Boolean restartKafkaConsumerFlag = false;
//    public static volatile Boolean stopEtlWithExceptionFlag = false;
//
//    @Autowired
//    @Qualifier("redisKafkaTemplate")
//    private RedisTemplate<String, ?> redisKafkaTemplate;
//
//    @Autowired
//    private TaCenteredAlertSendService taCenteredAlertSendService;
//
//    @Autowired
//    private MapperTaMysql mapperTaMysql;
//
//    private DateTime lastMailForOffsetReset = null;
//
//
//    // private Retryer retryerWithResultNull = RetryerUtil
//    // .initRetryerWithIfResult(Predicates.isNull());
//
//    // 4分钟重试，不行就退出
//    private Retryer retryerWithResultFails = RetryerUtil
//            .initRetryerWithStopTimeIfResult(240L, Predicates.equalTo(false));
//
//    // 无限重试
//    private Retryer retryer = cn.thinkingdata.ta.etl.util.RetryerUtil.initRetryer();
//
//
//    public KafkaConsumerOffset readOffset(String topic, Integer partition) {
//        return redisKafkaTemplate.execute(new RedisCallback<KafkaConsumerOffset>() {
//            @Override
//            public KafkaConsumerOffset doInRedis(RedisConnection redisConnection) {
//                try {
//                    if (partition == null || StringUtils.isBlank(topic)) {
//                        logger.error("read kafkaConsumerOffset from redis error, the topic is "
//                                + topic + ", the partition is " + partition);
//                        return null;
//                    }
//                    KafkaConsumerOffset kafkaConsumerOffset = new KafkaConsumerOffset();
//                    RedisSerializer<String> serializer = redisKafkaTemplate
//                            .getStringSerializer();
////                    KafkaMysqlOffsetParameter.kafkaClusterName = "tga";
////                    KafkaMysqlOffsetParameter.consumerGroup = "taRealtimeEtlGroup";
//                    String redisKeystr = KafkaMysqlOffsetParameter.kafkaClusterName
//                            + "-"
//                            + topic
//                            + "-"
//                            + partition
//                            + "-"
//                            + KafkaMysqlOffsetParameter.consumerGroup;
//                    byte[] redisKey = serializer.serialize(redisKeystr);
//                    Map<byte[], byte[]> kafkaConsumerOffsetMapByte = redisConnection.hGetAll(redisKey);
//                    if (MapUtils.isNotEmpty(kafkaConsumerOffsetMapByte)) {
//                        for (byte[] key : kafkaConsumerOffsetMapByte.keySet()) {
//                            if ("oid".equals(serializer.deserialize(key))) {
//                                byte[] oidByte = kafkaConsumerOffsetMapByte.get(key);
//                                if (oidByte != null) {
////                                    Integer oid = Integer.parseInt(serializer
////                                            .deserialize(oidByte));
//                                    kafkaConsumerOffset.setOid(Integer.parseInt(serializer.deserialize(oidByte)));
//                                }
//                            }
//                            if ("partition".equals(serializer.deserialize(key))) {
//                                byte[] partitionByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (partitionByte != null) {
//                                    kafkaConsumerOffset.setPartition(Integer.parseInt(serializer.deserialize(partitionByte)));
//                                }
//                            }
//                            if ("topic".equals(serializer.deserialize(key))) {
//                                byte[] topicByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (topicByte != null) {
//                                    kafkaConsumerOffset.setTopic(serializer.deserialize(topicByte));
//                                }
//                            }
//                            if ("consumer_group".equals(serializer.deserialize(key))) {
//                                byte[] consumer_groupByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (consumer_groupByte != null) {
//                                    kafkaConsumerOffset.setConsumer_group(serializer.deserialize(consumer_groupByte));
//                                }
//                            }
//                            if ("offset".equals(serializer.deserialize(key))) {
//                                byte[] offsetByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (offsetByte != null) {
//                                    kafkaConsumerOffset.setOffset(Long.parseLong(serializer.deserialize(offsetByte)));
//                                }
//                            }
//                            if ("last_flush_offset".equals(serializer.deserialize(key))) {
//                                byte[] last_flush_offsetByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (last_flush_offsetByte != null) {
//                                    kafkaConsumerOffset.setLast_flush_offset(Long.parseLong(serializer.deserialize(last_flush_offsetByte)));
//                                }
//                            }
//                            if ("kafka_cluster_name".equals(serializer.deserialize(key))) {
//                                byte[] kafka_cluster_nameByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (kafka_cluster_nameByte != null) {
//                                    kafkaConsumerOffset.setKafka_cluster_name(serializer.deserialize(kafka_cluster_nameByte));
//                                }
//                            }
//                            if ("owner".equals(serializer.deserialize(key))) {
//                                byte[] ownerByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (ownerByte != null) {
//                                    kafkaConsumerOffset.setOwner(serializer.deserialize(ownerByte));
//                                }
//                            }
//                            if ("update_time".equals(serializer.deserialize(key))) {
//                                byte[] update_timeByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (update_timeByte != null) {
//                                    kafkaConsumerOffset.setUpdate_time(new Date(Long.parseLong(serializer.deserialize(update_timeByte))));
//                                }
//                            }
//                            if ("create_time".equals(serializer.deserialize(key))) {
//                                byte[] create_timeByte = kafkaConsumerOffsetMapByte
//                                        .get(key);
//                                if (create_timeByte != null) {
//                                    kafkaConsumerOffset.setCreate_time(new Date(Long.parseLong(serializer.deserialize(create_timeByte))));
//                                }
//                            }
//                        }
//                    }
//                    if (!kafkaConsumerOffset.isNull()) {
//                        if (kafkaConsumerOffset.getCount() == null) {
//                            kafkaConsumerOffset.setCount(0L);
//                        }
//                    }
//                    logger.info("kafkaConsumerOffset in redis is " + kafkaConsumerOffset);
//                    return kafkaConsumerOffset;
//                } catch (Exception e) {
//                    logger.error("read kafkaConsumerOffset from redis error, the topic is "
//                            + topic
//                            + ", the partition is "
//                            + partition
//                            + ", the error is "
//                            + CommonUtils.getStackTraceAsString(e));
//                    return null;
//                }
//            }
//        });
//    }
//
//    // private Boolean clearOffset(String topic, Integer partition) {
//    // return redisTemplate.execute(new RedisCallback<Boolean>() {
//    // @Override
//    // public Boolean doInRedis(RedisConnection redisConnection) {
//    // try {
//    // String redisKeystr = KafkaMysqlOffsetParameter.kafkaClusterName
//    // + "-"
//    // + topic
//    // + "-"
//    // + partition
//    // + "-"
//    // + KafkaMysqlOffsetParameter.consumerGroup;
//    // RedisSerializer<String> serializer = redisTemplate
//    // .getStringSerializer();
//    // byte[] redisKey = serializer.serialize(redisKeystr);
//    // Long del = redisConnection.del(redisKey);
//    // if (del != 1L) {
//    // logger.error("clear kafkaConsumerOffset from redis error, the topic is "
//    // + topic
//    // + ", the partition is "
//    // + partition
//    // + ", the del number is " + del.toString());
//    // }
//    // return true;
//    // } catch (Exception e) {
//    // logger.error("clear kafkaConsumerOffset from redis error, the topic is "
//    // + topic
//    // + ", the partition is "
//    // + partition
//    // + ", the error is "
//    // + CommonUtils.getStackTraceAsString(e));
//    // return false;
//    // }
//    // }
//    // });
//    // }
//
//    private Boolean saveOffset(KafkaConsumerOffset kafkaConsumerOffset) {
//        return redisKafkaTemplate.execute(new RedisCallback<Boolean>() {
//            @Override
//            public Boolean doInRedis(RedisConnection redisConnection) {
//                try {
//                    if (kafkaConsumerOffset == null
//                            || kafkaConsumerOffset.isNull()) {
//                        logger.error("save offset fail in redis! kafkaConsumerOffset is "
//                                + kafkaConsumerOffset);
//                        return true;
//                    }
//                    RedisSerializer<String> serializer = redisKafkaTemplate
//                            .getStringSerializer();
//                    String redisKeystr = kafkaConsumerOffset
//                            .getKafka_cluster_name()
//                            + "-"
//                            + kafkaConsumerOffset.getTopic()
//                            + "-"
//                            + kafkaConsumerOffset.getPartition()
//                            + "-"
//                            + kafkaConsumerOffset.getConsumer_group();
//                    byte[] redisKey = serializer.serialize(redisKeystr);
//                    Map<byte[], byte[]> redisValue = new HashMap<byte[], byte[]>();
//                    if (kafkaConsumerOffset.getOid() != null) {
//                        redisValue.put(serializer.serialize("oid"), serializer
//                                .serialize(kafkaConsumerOffset.getOid()
//                                        .toString()));
//                    }
//                    redisValue.put(serializer.serialize("topic"), serializer
//                            .serialize(kafkaConsumerOffset.getTopic()));
//                    redisValue.put(serializer.serialize("partition"),
//                            serializer.serialize(kafkaConsumerOffset
//                                    .getPartition().toString()));
//                    redisValue.put(serializer.serialize("consumer_group"),
//                            serializer.serialize(kafkaConsumerOffset
//                                    .getConsumer_group()));
//                    redisValue.put(serializer.serialize("offset"), serializer
//                            .serialize(kafkaConsumerOffset.getOffset()
//                                    .toString()));
//                    redisValue.put(serializer.serialize("last_flush_offset"),
//                            serializer.serialize(kafkaConsumerOffset
//                                    .getLast_flush_offset().toString()));
//                    redisValue.put(serializer.serialize("kafka_cluster_name"),
//                            serializer.serialize(kafkaConsumerOffset
//                                    .getKafka_cluster_name()));
//                    redisValue.put(serializer.serialize("owner"), serializer
//                            .serialize(kafkaConsumerOffset.getOwner()));
//                    redisValue.put(serializer.serialize("update_time"),
//                            serializer.serialize(Long
//                                    .toString(kafkaConsumerOffset
//                                            .getUpdate_time().getTime())));
//                    redisValue.put(serializer.serialize("create_time"),
//                            serializer.serialize(Long
//                                    .toString(kafkaConsumerOffset
//                                            .getCreate_time().getTime())));
//                    redisConnection.hMSet(redisKey, redisValue);
//                    return true;
//                } catch (Exception e) {
//                    logger.error("write kafkaConsumerOffset"
//                            + kafkaConsumerOffset.toString()
//                            + " to redis error, the error is "
//                            + CommonUtils.getStackTraceAsString(e));
//                    return false;
//                }
//
//            }
//        });
//    }
//
//    // 去掉synchronized，因为MysqlOffsetPersist的flush和persist里有synchronized方法
//    @Override
//    public Boolean saveOffsetInBackupExternalStore(
//            KafkaConsumerOffset kafkaConsumerOffset) {
//        Long lag = kafkaConsumerOffset.getOffset()
//                - kafkaConsumerOffset.getLast_flush_offset();
//        if (!lag.equals(0L)) {
//            logger.debug("because of the muti-thread, the value is not exactly right, the lag is "
//                    + lag);
//            return saveOffset(kafkaConsumerOffset);
//        }
//        return true;
//    }
//
//    @Override
//    public KafkaConsumerOffset readOffsetFromBackupExternalStore(String topic,
//                                                                 int partition) {
//        KafkaConsumerOffset kafkaConsumerOffsetFromBackupExternalStore = readOffset(
//                topic, partition);
//        return kafkaConsumerOffsetFromBackupExternalStore;
//    }
//
//    // @Override
//    // public synchronized Boolean clearOffsetFromBackupExternalStore(
//    // String topic, int partition) {
//    // return clearOffset(topic, partition);
//    // }
//
//    @Override
//    public KafkaConsumerOffset executeWhenReadNullFromBackupExternalStore(
//            String topic, Integer partition) {
//        // KafkaConsumerOffset kafkaConsumerOffsetFromBackupExternalStore =
//        // null;
//        // try {
//        // kafkaConsumerOffsetFromBackupExternalStore = (KafkaConsumerOffset)
//        // retryerWithResultNull
//        // .call(new Callable() {
//        // @Override
//        // public Object call() throws Exception {
//        // return readOffset(topic, partition);
//        // }
//        // });
//        // } catch (ExecutionException | RetryException e) {
//        // logger.error("retry to read kafkaConsumerOffset from redis error, the error is "
//        // + CommonUtils.getStackTraceAsString(e));
//        // }
//        // return kafkaConsumerOffsetFromBackupExternalStore;
//        logger.error("can not read offset from redis! exit the process!");
//        stopEtlWithExceptionFlag = true;
//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException e) {
//            logger.error("------- thread can not sleep ---------------------"
//                    + e.toString());
//        }
//        return null;
//    }
//
//    @Override
//    public KafkaConsumerOffset executeWhenReadNullFromMysql(String topic,
//                                                            Integer partition) {
//        // KafkaConsumerOffset kafkaConsumerOffset = null;
//        // try {
//        // kafkaConsumerOffset = (KafkaConsumerOffset) retryerWithResultNull
//        // .call(new Callable() {
//        // @Override
//        // public Object call() throws Exception {
//        // return MysqlOffsetManager.getInstance()
//        // .readOffsetFromMysql(topic, partition);
//        // }
//        // });
//        // if (kafkaConsumerOffset == null) {
//        // logger.error("the kafkaConsumerOffset read from mysql is null , the topic is "
//        // + topic
//        // + "the partition is "
//        // + partition
//        // + "exit the process unclear!");
//        // System.exit(-1);
//        // }
//        // } catch (ExecutionException | RetryException e) {
//        // logger.error("the kafkaConsumerOffset read from mysql is null , the topic is "
//        // + topic
//        // + "the partition is "
//        // + partition
//        // + "exit the process unclear! the error is "
//        // + CommonUtils.getStackTraceAsString(e));
//        // System.exit(-1);
//        // return null;
//        // }
//        // return kafkaConsumerOffset;
//        logger.error("can not read offset from mysql! exit the process!");
//        stopEtlWithExceptionFlag = true;
//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException e) {
//            logger.error("------- thread can not sleep ---------------------"
//                    + e.toString());
//        }
//        return null;
//    }
//
//    // @Override
//    // public Boolean executeWhenNotClearExternalStore(final String topic,
//    // final Integer partition) {
//    // Boolean flag = false;
//    // try {
//    // flag = (Boolean) retryerWithResultFails.call(new Callable() {
//    // @Override
//    // public Object call() throws Exception {
//    // return clearOffsetFromBackupExternalStore(topic, partition);
//    // }
//    // });
//    // if (!flag) {
//    // logger.error("retry to clear kafkaConsumerOffset from backup external store error!");
//    // }
//    // } catch (ExecutionException | RetryException e) {
//    // logger.error("retry to clear kafkaConsumerOffset from backup external store error, the error is "
//    // + CommonUtils.getStackTraceAsString(e));
//    // return false;
//    // }
//    // return flag;
//    // }
//
//    @Override
//    public Boolean executeWhenSaveOffsetFailInMysqlAndExternalStore(
//            final KafkaConsumerOffset kafkaConsumerOffset) {
//        logger.error("save offset fail in mysql and redis! retry!");
//        Boolean flag = false;
//        try {
//            flag = (Boolean) retryerWithResultFails.call(new Callable() {
//                @Override
//                public Object call() throws Exception {
//                    return MysqlOffsetPersist.getInstance().saveOffset(
//                            kafkaConsumerOffset);
//                }
//            });
//            if (!flag) {
//                logger.error("save offset fail in mysql and redis! exit the process unclear!");
//                stopEtlWithExceptionFlag = true;
//                try {
//                    Thread.sleep(100000);
//                } catch (InterruptedException e) {
//                    logger.error("------- thread can not sleep ---------------------"
//                            + e.toString());
//                }
//                return null;
//            }
//        } catch (ExecutionException | RetryException e) {
//            logger.error("save offset fail in mysql and redis! exit the process unclear! the error is "
//                    + CommonUtils.getStackTraceAsString(e));
//            stopEtlWithExceptionFlag = true;
//            try {
//                Thread.sleep(100000);
//            } catch (InterruptedException e2) {
//                logger.error("------- thread can not sleep ---------------------"
//                        + e2.toString());
//            }
//            return null;
//        }
//        return flag;
//    }
//
//    @Override
//    public Boolean backupStoreStateCheck() {
//        try {
//            return redisKafkaTemplate.execute(new RedisCallback<Boolean>() {
//                @Override
//                public Boolean doInRedis(RedisConnection redisConnection) {
//                    try {
//                        Boolean closed = redisConnection.isClosed();
//                        if (!closed) {
//                            logger.info("redis reconnected!");
//                        } else {
//                            logger.info("redis session closed!");
//                        }
//                        return !closed;
//                    } catch (Exception e) {
//                        logger.info("redis connect error, the error is " + e);
//                        return false;
//                    }
//                }
//            });
//
//        } catch (Exception e) {
//            logger.error("redis connect error, the error is " + e);
//            return false;
//        }
//
//    }
//
//    @Override
//    public Boolean updateOwner(KafkaConsumerOffset kafkaConsumerOffset) {
//        Date now = new Date();
//        if (kafkaConsumerOffset.getOffset() == 0L) {
//            kafkaConsumerOffset.setUpdate_time(now);
//            if (kafkaConsumerOffset.getCreate_time() == null) {
//                kafkaConsumerOffset.setCreate_time(now);
//            }
//        }
//        return updateOwnerInRedis(kafkaConsumerOffset);
//    }
//
//    private Boolean updateOwnerInRedis(KafkaConsumerOffset kafkaConsumerOffset) {
//        try {
//            return redisKafkaTemplate.execute(new RedisCallback<Boolean>() {
//                @Override
//                public Boolean doInRedis(RedisConnection redisConnection) {
//
//                    if (kafkaConsumerOffset == null
//                            || kafkaConsumerOffset.isNull()) {
//                        logger.error("save offset fail in redis! kafkaConsumerOffset is "
//                                + kafkaConsumerOffset);
//                        return true;
//                    }
//                    RedisSerializer<String> serializer = redisKafkaTemplate
//                            .getStringSerializer();
//                    String redisKeystr = kafkaConsumerOffset
//                            .getKafka_cluster_name()
//                            + "-"
//                            + kafkaConsumerOffset.getTopic()
//                            + "-"
//                            + kafkaConsumerOffset.getPartition()
//                            + "-"
//                            + kafkaConsumerOffset.getConsumer_group();
//                    byte[] redisKey = serializer.serialize(redisKeystr);
//                    Map<byte[], byte[]> redisValue = new HashMap<byte[], byte[]>();
//                    if (kafkaConsumerOffset.getOid() != null) {
//                        redisValue.put(serializer.serialize("oid"), serializer
//                                .serialize(kafkaConsumerOffset.getOid()
//                                        .toString()));
//                    }
//                    redisValue.put(serializer.serialize("topic"), serializer
//                            .serialize(kafkaConsumerOffset.getTopic()));
//                    redisValue.put(serializer.serialize("partition"),
//                            serializer.serialize(kafkaConsumerOffset
//                                    .getPartition().toString()));
//                    redisValue.put(serializer.serialize("consumer_group"),
//                            serializer.serialize(kafkaConsumerOffset
//                                    .getConsumer_group()));
//                    redisValue.put(serializer.serialize("offset"), serializer
//                            .serialize(kafkaConsumerOffset.getOffset()
//                                    .toString()));
//                    redisValue.put(serializer.serialize("last_flush_offset"),
//                            serializer.serialize(kafkaConsumerOffset
//                                    .getLast_flush_offset().toString()));
//                    redisValue.put(serializer.serialize("kafka_cluster_name"),
//                            serializer.serialize(kafkaConsumerOffset
//                                    .getKafka_cluster_name()));
//                    redisValue.put(serializer.serialize("owner"), serializer
//                            .serialize(kafkaConsumerOffset.getOwner()));
//                    redisValue.put(serializer.serialize("update_time"),
//                            serializer.serialize(Long
//                                    .toString(kafkaConsumerOffset
//                                            .getUpdate_time().getTime())));
//                    redisValue.put(serializer.serialize("create_time"),
//                            serializer.serialize(Long
//                                    .toString(kafkaConsumerOffset
//                                            .getCreate_time().getTime())));
//                    redisConnection.hMSet(redisKey, redisValue);
//                    return true;
//
//
//                }
//            });
//        } catch (Exception e) {
//            logger.error("updateOwner in redis error, the error is " + CommonUtils.getStackTraceAsString(e));
//            return false;
//        }
//    }
//
//    @Override
//    public void executeWhenSessionTimeout(Integer count) {
//        logger.error("session will time out! the count is " + count
//                + ", the session time out is "
//                + KafkaMysqlOffsetParameter.sessionTimeout + ",exit unclear!");
//        stopEtlWithExceptionFlag = true;
//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException e) {
//            logger.error("------- thread can not sleep ---------------------"
//                    + e.toString());
//        }
//    }
//
//    @Override
//    public void executeWhenExecuteDataSessionTimeout(KafkaSubscribeConsumeThread kafkaSubscribeConsumeThread) {
//        logger.error("session time out!" + ", the session time out is "
//                + KafkaMysqlOffsetParameter.sessionTimeout);
//        restartKafkaConsumerFlag = true;
//    }
//
//    @Override
//    public void executeWhenOffsetReset() {
//        logger.error("kafka consumer offset has no such offset!");
//        try {
//            if(lastMailForOffsetReset == null || (lastMailForOffsetReset != null && lastMailForOffsetReset.plusMinutes(3).isBeforeNow())){
//                Do_ta_company_info companyInfo = mapperTaMysql.getCompanyInfo();
//                String mailContent = "公司名称：" + companyInfo.getCompany_name() + "<br>kafka Offset reset";
//                taCenteredAlertSendService.alertByMail("ta_alert@thinkingdata.cn",null,"etl因为kafka的offset的reset而暂停60秒",mailContent);
//                lastMailForOffsetReset = new DateTime();
//            }
//            Thread.sleep(3000);
//        } catch (Exception e) {
//            logger.error("send mail to ta_alert@thinkingdata.cn, " + Throwables.getStackTraceAsString(e));
//        }
//    }
//
//    @Override
//    public void executeWhenException() {
//        logger.error("kafka consumer offset reset, exit unclear");
//        try {
//            Do_ta_company_info companyInfo = mapperTaMysql.getCompanyInfo();
//            String mailContent = "公司名称：" + companyInfo.getCompany_name() + "<br>kafka读取异常,请人工介入";
//            taCenteredAlertSendService.alertByMail("ta_alert@thinkingdata.cn",null,"etl因为kafka的offset异常退出",mailContent);
//        } catch (Exception e) {
//            logger.error("send mail to ta_alert@thinkingdata.cn, " + Throwables.getStackTraceAsString(e));
//        }
//        // do not let monitor restart
////        try {
////            setKeyVal(TaConstants.ETL_RESET_MARK, "1");
////        } catch (ExecutionException | RetryException e) {
////            logger.error("set etl reset mark error, " + e.toString());
////        }
//        stopEtlWithExceptionFlag = true;
//        try {
//            Thread.sleep(80000);
//        } catch (InterruptedException e) {
//            logger.error("------- thread can not sleep ---------------------" + e.toString());
//        }
//    }
//
////    public Object setKeyVal(String key, String value) throws ExecutionException, RetryException {
////        return retryer.call(new Callable() {
////            @Override
////            public Boolean call() throws Exception {
////                return redisKafkaTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
////                    RedisSerializer<String> serializer = redisKafkaTemplate.getStringSerializer();
////                    return redisConnection.set(serializer.serialize(key), serializer.serialize(value));
////                });
////            }
////        });
////    }
//
//
//}
//
//
//
