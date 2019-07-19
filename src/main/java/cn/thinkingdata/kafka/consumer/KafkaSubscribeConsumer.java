package cn.thinkingdata.kafka.consumer;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.close.DaemonCloseThread;
import cn.thinkingdata.kafka.close.TermMethod;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.offset.MysqlOffsetManager;
import cn.thinkingdata.kafka.consumer.persist.MysqlOffsetPersist;
import cn.thinkingdata.kafka.consumer.persist.StorePersist;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaSubscribeConsumer {

    private static final Logger logger = LoggerFactory
            .getLogger(KafkaSubscribeConsumer.class);

    protected IDataLineProcessor dataProcessor;
    protected volatile ExecutorService executorService;
    private TermMethod closeMethod;
    private volatile DaemonCloseThread closeSignal;
    private static volatile Integer startCount = 0;

    // 同步offset的size，同步offset的时间
    public KafkaSubscribeConsumer(String jdbcUrl, String username,
                                  String password, String tableName, String brokerList,
                                  String kafkaClusterName, String topic, String consumerGroup,
                                  IDataLineProcessor dataProcessor, Integer processThreadNum,
                                  Integer flushOffsetSize, Integer flushInterval,
                                  TermMethod closeMethod) throws IOException {
        KafkaMysqlOffsetParameter.setValue(jdbcUrl, username, password,
                tableName, brokerList, kafkaClusterName, topic, consumerGroup,
                processThreadNum, flushOffsetSize, flushInterval);
        this.dataProcessor = dataProcessor;
        this.closeMethod = closeMethod;
        KafkaMysqlOffsetParameter.createKafkaConfProp();
    }

    public KafkaSubscribeConsumer(String jdbcUrl, String username,
                                  String password, String tableName, String brokerList,
                                  String kafkaClusterName, String topic, String consumerGroup,
                                  IDataLineProcessor dataProcessor, Integer processThreadNum,
                                  Integer flushOffsetSize, Integer flushInterval,
                                  TermMethod closeMethod, StorePersist externalStorePersist)
            throws IOException {
        this(jdbcUrl, username, password, tableName, brokerList,
                kafkaClusterName, topic, consumerGroup, dataProcessor,
                processThreadNum, flushOffsetSize, flushInterval, closeMethod);
        MysqlOffsetManager.getInstance().setExternalStorePersist(
                externalStorePersist);
    }

    public KafkaSubscribeConsumer(String jdbcUrl, String username,
                                  String password, String tableName, String brokerList,
                                  String kafkaClusterName, String topic, String consumerGroup,
                                  IDataLineProcessor dataProcessor, Integer processThreadNum,
                                  Integer flushOffsetSize, Integer flushInterval,
                                  Integer pollInterval, Properties kafkaConf, TermMethod closeMethod)
            throws IOException {
        this(jdbcUrl, username, password, tableName, brokerList,
                kafkaClusterName, topic, consumerGroup, dataProcessor,
                processThreadNum, flushOffsetSize, flushInterval, closeMethod);
        KafkaMysqlOffsetParameter.setPollInterval(pollInterval);
        KafkaMysqlOffsetParameter.createKafkaConfProp(kafkaConf);
    }

    public KafkaSubscribeConsumer(String jdbcUrl, String username,
                                  String password, String tableName, String brokerList,
                                  String kafkaClusterName, String topic, String consumerGroup,
                                  IDataLineProcessor dataProcessor, Integer processThreadNum,
                                  Integer flushOffsetSize, Integer flushInterval,
                                  Integer pollInterval, Properties kafkaConf, TermMethod closeMethod,
                                  StorePersist externalStorePersist) throws IOException {
        this(jdbcUrl, username, password, tableName, brokerList,
                kafkaClusterName, topic, consumerGroup, dataProcessor,
                processThreadNum, flushOffsetSize, flushInterval, pollInterval,
                kafkaConf, closeMethod);
        MysqlOffsetManager.getInstance().setExternalStorePersist(
                externalStorePersist);
    }

    public KafkaSubscribeConsumer(String jdbcUrl, String username,
                                  String password, String tableName, String brokerList,
                                  String kafkaClusterName, String topic, String consumerGroup,
                                  IDataLineProcessor dataProcessor, Integer processThreadNum,
                                  Integer flushOffsetSize, Integer flushInterval,
                                  Integer pollInterval, Long maxPartitionFetchBytes,
                                  Integer heartbeatInterval, Integer sessionTimeout,
                                  Integer requestTimeout, String autoOffsetReset,
                                  TermMethod closeMethod) throws IOException {
        this(jdbcUrl, username, password, tableName, brokerList,
                kafkaClusterName, topic, consumerGroup, dataProcessor,
                processThreadNum, flushOffsetSize, flushInterval, closeMethod);
        KafkaMysqlOffsetParameter.setPollInterval(pollInterval);
        KafkaMysqlOffsetParameter.createKafkaConfProp(maxPartitionFetchBytes,
                heartbeatInterval, sessionTimeout, requestTimeout, autoOffsetReset);
    }

    public KafkaSubscribeConsumer(String jdbcUrl, String username,
                                  String password, String tableName, String brokerList,
                                  String kafkaClusterName, String topic, String consumerGroup,
                                  IDataLineProcessor dataProcessor, Integer processThreadNum,
                                  Integer flushOffsetSize, Integer flushInterval,
                                  Integer pollInterval, Long maxPartitionFetchBytes,
                                  Integer heartbeatInterval, Integer sessionTimeout,
                                  Integer requestTimeout, String autoOffsetReset,
                                  TermMethod closeMethod, StorePersist externalStorePersist)
            throws IOException {
        this(jdbcUrl, username, password, tableName, brokerList,
                kafkaClusterName, topic, consumerGroup, dataProcessor,
                processThreadNum, flushOffsetSize, flushInterval, pollInterval,
                maxPartitionFetchBytes, heartbeatInterval, sessionTimeout,
                requestTimeout, autoOffsetReset, closeMethod);
        MysqlOffsetManager.getInstance().setExternalStorePersist(
                externalStorePersist);
    }

    public KafkaSubscribeConsumer(String jdbcUrl, String username,
                                  String password, String tableName, String brokerList,
                                  String kafkaClusterName, String topic, String consumerGroup,
                                  IDataLineProcessor dataProcessor, Integer processThreadNum,
                                  Integer flushOffsetSize, Integer flushInterval,
                                  Integer pollInterval, Long maxPartitionFetchBytes,
                                  Integer heartbeatInterval, Integer sessionTimeout,
                                  Integer requestTimeout, String autoOffsetReset,
                                  TermMethod closeMethod, StorePersist externalStorePersist, Integer maxPollRecords)
            throws IOException {
        this(jdbcUrl, username, password, tableName, brokerList,
                kafkaClusterName, topic, consumerGroup, dataProcessor,
                processThreadNum, flushOffsetSize, flushInterval, pollInterval,
                maxPartitionFetchBytes, heartbeatInterval, sessionTimeout,
                requestTimeout, autoOffsetReset, closeMethod);
        KafkaMysqlOffsetParameter.maxPollRecords = maxPollRecords;
        MysqlOffsetManager.getInstance().setExternalStorePersist(
                externalStorePersist);
    }

    public void run() {
        //判断mysql和redis是否通
        Boolean mysqlStateCheck = MysqlOffsetPersist.getInstance().mysqlStateCheckWithRetry();
        if(!mysqlStateCheck){
            logger.info("mysql is not connected!");
            System.exit(-1);
        }
        Boolean backupStoreStateCheck = MysqlOffsetPersist.getInstance().backupStoreStateCheckWithRetry();
        if(!backupStoreStateCheck){
            logger.info("backup store is not connected!");
            System.exit(-1);
        }
        KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed.set(false);
        List<String> topicList = new ArrayList<String>();
        topicList.add(KafkaMysqlOffsetParameter.topic);
        executorService = Executors
                .newFixedThreadPool(KafkaMysqlOffsetParameter.processThreadNum);
        CyclicBarrier offsetFlushBarrier = new CyclicBarrier(
                KafkaMysqlOffsetParameter.processThreadNum);
        for (int i = 0; i < KafkaMysqlOffsetParameter.processThreadNum; i++) {
            KafkaSubscribeConsumerManager kafkaSubscribeConsumer = KafkaSubscribeConsumerManager
                    .getInstance();
            KafkaConsumer<String, String> consumer = kafkaSubscribeConsumer
                    .createKafkaConsumer(topicList,
                            KafkaMysqlOffsetParameter.kafkaConf);
            KafkaSubscribeConsumeThread consumeThread = new KafkaSubscribeConsumeThread(
                    consumer, dataProcessor, offsetFlushBarrier);
            KafkaCache.consumeThreadList.add(consumeThread);
            executorService.submit(consumeThread);
        }
        // 启动定时刷数据入mysql
        if(startCount.equals(0)){
            MysqlOffsetPersist.getInstance().start();
            startCount = startCount + 1;
        }
        closeSignal = new DaemonCloseThread(this, closeMethod);
        closeSignal.setDaemon(true);
        closeSignal.start();
    }

    public void stopWithException() {
        logger.info("consumers start shutdown");
        for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
            if(consumeThread != null){
                consumeThread.shutdown();
                consumeThread.processDataWorker.stopWithException();
            }
        }
        try {
            // 等待所有拉取线程自动停止
            Thread.sleep(5000);
            // 等待所有consumer关闭
            Thread.sleep(5000);
            // 等待所有consumer的working线程关闭
            Thread.sleep(5000);
        } catch (InterruptedException e2) {
            logger.error("------- thread can not sleep ---------------------"
                    + e2.toString());
        }
        // 关闭线程池
        if (executorService != null)
            executorService.shutdown();
        try {
            if (!executorService.awaitTermination(120000, TimeUnit.MILLISECONDS)) {
                logger.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted during shutdown, exiting uncleanly");
        }
        logger.info("dataProcessor start to shutdown");
        dataProcessor.finishProcess();
        closeSignal.shutdown();
    }

    public void destroyWithException(){
        MysqlOffsetPersist.destoryFlag = true;
        stopWithException();
        closeSignal.afterDestroyConsumer();
        logger.info("mysql start to shutdown");
        // 关闭mysql连接
        MysqlOffsetPersist.getInstance().shutdown();
    }

    public void stop() {
        logger.info("consumers start shutdown");
        for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
            if(consumeThread != null){
                consumeThread.shutdown();
            }
        }
        // 等待所有拉取线程自动停止
        for (;;) {
            Boolean kafkaPollFlag = false;
            for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
                if (consumeThread != null && consumeThread.kafkaPollFlag) {
                    kafkaPollFlag = true;
                }
            }
            if (!kafkaPollFlag) {
                break;
            }
        }
        logger.info("kafka polling closed");
        // 等待所有consumer关闭
        for (;;) {
            Boolean kafkaConsumerFlag = false;
            for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
                if (consumeThread != null && consumeThread.kafkaConsumerFlag) {
                    kafkaConsumerFlag = true;
                }
            }
            if (!kafkaConsumerFlag) {
                break;
            }
        }
        logger.info("kafka consumer closed");
        // 等待所有consumer的working线程关闭
        for (;;) {
            Boolean processDataWorkingFlag = false;
            for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
                if (consumeThread != null && consumeThread.processDataWorker.workingFlag) {
                    processDataWorkingFlag = true;
                }
            }
            if (!processDataWorkingFlag) {
                break;
            }
        }
        logger.info("process data worker closed");
        // 关闭线程池
        if (executorService != null)
            executorService.shutdown();
        try {
            if (!executorService.awaitTermination(120000, TimeUnit.MILLISECONDS)) {
                logger.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted during shutdown, exiting uncleanly");
        }
        logger.info("dataProcessor start to shutdown");
        dataProcessor.finishProcess();
        closeSignal.shutdown();
    }

    public void destroy() {
        MysqlOffsetPersist.destoryFlag = true;
        stop();
        closeSignal.afterDestroyConsumer();
        logger.info("mysql start to shutdown");
        // 关闭mysql连接
        MysqlOffsetPersist.getInstance().shutdown();
    }

}
