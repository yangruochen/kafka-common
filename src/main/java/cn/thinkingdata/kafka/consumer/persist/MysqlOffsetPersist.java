package cn.thinkingdata.kafka.consumer.persist;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.offset.MysqlOffsetManager;
import cn.thinkingdata.kafka.util.CommonUtils;
import cn.thinkingdata.kafka.util.RetryerUtil;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.common.base.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class MysqlOffsetPersist extends Thread implements OffsetPersist {

    private static final Logger logger = LoggerFactory
            .getLogger(MysqlOffsetPersist.class);

    private static MysqlOffsetPersist instance;
    public static volatile Boolean destoryFlag = false;
    public static volatile Boolean runFlag = false;

    private StorePersist externalStorePersist = MysqlOffsetManager
            .getInstance().getExternalStorePersist();

    // public static Boolean mysqlOffsetPersistFlag = false;

    public static synchronized MysqlOffsetPersist getInstance() {
        if (instance == null) {
            instance = new MysqlOffsetPersist();
        }
        return instance;
    }

    private Retryer retryerWithResultFails = RetryerUtil
            .initRetryerByTimesWithIfResult(3, 300, Predicates.equalTo(false));

    private MysqlOffsetPersist() {
    }

    // persist和flush互斥
    @Override
    public void persist(KafkaConsumerOffset kafkaConsumerOffset) {
        Boolean saveOffsetFlag = saveOffset(kafkaConsumerOffset);

        if (!saveOffsetFlag) {
            logger.error("can not persist in both mysql or backup store");
            externalStorePersist
                    .executeWhenSaveOffsetFailInMysqlAndExternalStore(kafkaConsumerOffset);
        }
    }

    @Override
    public void shutdown() {
        // 关闭定时线程
        logger.info("------- Shutting mysql offset thread Down ---------------------");
        MysqlOffsetManager.getInstance().shutdown();
    }

    // @Override
    // public synchronized void flush() {
    // logger.info("------- flush all offset in cache to mysql ---------------------");
    // MysqlOffsetManager.getInstance().saveAllOffsetInCacheToMysql();
    // KafkaCache.kafkaConsumerOffsets.clear();
    // }

    public void mysqlAndBackupStoreStateCheckJob() {
        // 如果mysql和BackupStoreState连接不通，看看有没有恢复
        int count = -1;
        while (!KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState.get()) {
            if (count == -1) {
                logger.info("------- mysql or backup store down, check mysql and backup store status ---------------------");
                count = 0;
            }
            Boolean mysqlStateCheck = mysqlStateCheckWithRetry();
            Boolean backupStoreStateCheck = backupStoreStateCheckWithRetry();
            if (!mysqlStateCheck || !backupStoreStateCheck) {
                count++;
                // 如果mysql或者back up store超过sessionTimeout，就要停止
                if (count > Integer
                        .parseInt(KafkaMysqlOffsetParameter.sessionTimeout) - 10) {
                    externalStorePersist.executeWhenSessionTimeout(count);
                }
            } else {
                count = 0;
            }
            if (mysqlStateCheck || backupStoreStateCheck) {
                KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState
                        .set(true);
            } else {
                KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState
                        .set(false);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("------- thread can not sleep ---------------------"
                        + e.toString());
            }
        }
    }

    public Boolean mysqlStateCheckWithRetry() {
        Boolean flag = false;
        try {
            flag = (Boolean) retryerWithResultFails.call(new Callable() {

                @Override
                public Object call() throws Exception {
                    return MysqlOffsetManager.getInstance().mysqlStateCheck();
                }
            });
        } catch (ExecutionException | RetryException e) {
            logger.error("retry mysqlStateCheck error, the error is "
                    + CommonUtils.getStackTraceAsString(e));
            flag = false;
        }
        return flag;
    }

    public Boolean backupStoreStateCheckWithRetry() {
        Boolean flag = false;
        try {
            flag = (Boolean) retryerWithResultFails.call(new Callable() {

                @Override
                public Object call() throws Exception {
                    return externalStorePersist.backupStoreStateCheck();
                }
            });
        } catch (ExecutionException | RetryException e) {
            logger.error("retry backupStoreStateCheck error, the error is "
                    + CommonUtils.getStackTraceAsString(e));
            flag = false;
        }
        return flag;
    }

    @Override
    public void run() {
        while (!destoryFlag) {
            if (!KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed.get()) {
                runFlag = true;
                mysqlAndBackupStoreStateCheckJob();
                // 如果通，并且没有进行rebalance，则定时刷数据进mysql
                if (KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState.get()) {
                    persisit();
                }
            }
            runFlag = false;
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

    public Boolean saveOffset(final KafkaConsumerOffset kafkaConsumerOffset) {

        Boolean flagMysqlStore = false;
        Date now = new Date();
        // 更新的Update_time
        kafkaConsumerOffset.setUpdate_time(now);
        if (kafkaConsumerOffset.getCreate_time() == null)
            kafkaConsumerOffset.setCreate_time(now);
        // 得到Last_flush_offset防止consumeThread线程修改数据
        Long last_flush_offset = kafkaConsumerOffset.getOffset();
        try {
            flagMysqlStore = (Boolean) retryerWithResultFails
                    .call(new Callable() {

                        @Override
                        public Object call() throws Exception {
                            return MysqlOffsetManager.getInstance()
                                    .saveOffsetInCacheToMysql(
                                            kafkaConsumerOffset);
                        }
                    });
        } catch (ExecutionException | RetryException e) {
            logger.error("retry to save kafkaConsumerOffset to mysql and backup external store error, the error is "
                    + CommonUtils.getStackTraceAsString(e));
            flagMysqlStore = false;
        }

        Boolean flagBackupStore = false;
        try {
            // 写一个存到备用存储的接口，默认是空
            flagBackupStore = (Boolean) retryerWithResultFails
                    .call(new Callable() {

                        @Override
                        public Object call() throws Exception {
                            return MysqlOffsetManager
                                    .getInstance()
                                    .getExternalStorePersist()
                                    .saveOffsetInBackupExternalStore(
                                            kafkaConsumerOffset);
                        }
                    });

        } catch (ExecutionException | RetryException e) {
            logger.error("retry to save kafkaConsumerOffset to mysql and backup external store error, the error is "
                    + CommonUtils.getStackTraceAsString(e));
            flagBackupStore = false;
        }

        Boolean saveOffsetFlag = flagMysqlStore || flagBackupStore;
        if (saveOffsetFlag) {
            kafkaConsumerOffset.setLast_flush_offset(last_flush_offset);
            kafkaConsumerOffset.setCount(0L);
            KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState.set(true);
        } else {
            KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState.set(false);
        }
        return saveOffsetFlag;

    }

    @Override
    public synchronized Boolean flush(KafkaConsumerOffset kafkaConsumerOffset) {
        logger.info("------- flush offset in cache to mysql ---------------------");

        Boolean saveOffsetFlag = saveOffset(kafkaConsumerOffset);

        if (!saveOffsetFlag) {
            logger.error("can not flush in mysql or backup store");
            externalStorePersist
                    .executeWhenSaveOffsetFailInMysqlAndExternalStore(kafkaConsumerOffset);
        }

        KafkaCache.kafkaConsumerOffsets.remove(kafkaConsumerOffset);

        kafkaConsumerOffset.setOwner("");
        updateOwner(kafkaConsumerOffset);

        return saveOffsetFlag;

    }

//    public Boolean deleteOwner(
//            final KafkaConsumerOffset kafkaConsumerOffset) {
//        try {
//            return (Boolean) retryerWithResultFails.call(new Callable() {
//                @Override
//                public Object call() throws Exception {
//                    return MysqlOffsetManager.getInstance().updateOwner(
//                            kafkaConsumerOffset);
//                }
//            });
//        } catch (ExecutionException | RetryException e) {
//            logger.error("retry to deleteOwner from mysql error, the error is "
//                    + CommonUtils.getStackTraceAsString(e));
//            return false;
//        }
//
//    }

    public synchronized Boolean updateOwner(final KafkaConsumerOffset kafkaConsumerOffset) {
        try {
            Boolean flagMysqlStore = false;
            flagMysqlStore = (Boolean) retryerWithResultFails.call(new Callable() {
                @Override
                public Object call() throws Exception {
                    return MysqlOffsetManager.getInstance().updateOwner(
                            kafkaConsumerOffset);
                }
            });
            Boolean flagBackupStore = false;
            flagBackupStore = (Boolean) retryerWithResultFails.call(new Callable() {
                @Override
                public Object call() throws Exception {
                    return MysqlOffsetManager.getInstance().getExternalStorePersist().updateOwner(
                            kafkaConsumerOffset);
                }
            });
            Boolean flag = flagMysqlStore || flagBackupStore;
            return flag;
        } catch (ExecutionException | RetryException e) {
            logger.error("retry to updateOwner from mysql error, the error is "
                    + CommonUtils.getStackTraceAsString(e));
            return false;
        }
    }
}
