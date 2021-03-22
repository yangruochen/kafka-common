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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaSubscribeConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSubscribeConsumer.class);

    protected NewIDataLineProcessor dataProcessor;
    protected volatile ExecutorService executorService;
    private final TermMethod closeMethod;
    private volatile DaemonCloseThread closeSignal;
    private static volatile Integer startCount = 0;

    public KafkaSubscribeConsumer(Map<String, String> map, NewIDataLineProcessor dataProcessor, TermMethod closeMethod) {
        KafkaMysqlOffsetParameter.createKafkaConfProp(map);
        this.dataProcessor = dataProcessor;
        this.closeMethod = closeMethod;
    }

    public KafkaSubscribeConsumer(Map<String, String> map, NewIDataLineProcessor dataProcessor, TermMethod closeMethod, StorePersist externalStorePersist){
        this(map, dataProcessor, closeMethod);
        MysqlOffsetManager.getInstance().setExternalStorePersist(externalStorePersist);
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
        List<String> topicList = new ArrayList();
        topicList.addAll(Arrays.asList(KafkaMysqlOffsetParameter.topic.split(",")));
        executorService = Executors.newFixedThreadPool(KafkaMysqlOffsetParameter.processThreadNum);
        CyclicBarrier offsetFlushBarrier = new CyclicBarrier(KafkaMysqlOffsetParameter.processThreadNum);
        for (int i = 0; i < KafkaMysqlOffsetParameter.processThreadNum; i++) {
            KafkaSubscribeConsumerManager kafkaSubscribeConsumer = KafkaSubscribeConsumerManager.getInstance();
            KafkaConsumer<String, String> consumer = kafkaSubscribeConsumer.createKafkaConsumer(topicList, KafkaMysqlOffsetParameter.kafkaConf);
            KafkaSubscribeConsumeThread consumeThread = new KafkaSubscribeConsumeThread(consumer, dataProcessor, offsetFlushBarrier);
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

    public void stop() {
        stop(120000);
    }

    public void stop(long stopTimeOut) {
        long startTime = System.currentTimeMillis();
        logger.info("consumers start shutdown");
        for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
            if(consumeThread != null){
                consumeThread.shutdown();
            }
        }
        Boolean stopExceptionFlag = false;
        // 等待所有拉取线程自动停止
        for (;;) {
            if(stopExceptionFlag){
                break;
            }
            Boolean kafkaPollFlag = false;
            for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
                if (consumeThread != null && consumeThread.kafkaPollFlag) {
                    kafkaPollFlag = true;
                }
            }
            if (!kafkaPollFlag) {
                break;
            }
            if(System.currentTimeMillis()-startTime > stopTimeOut){
                stopWithTimeOUt();
                stopExceptionFlag = true;
            }
        }
        logger.info("kafka polling closed");
        // 等待所有consumer关闭
        for (;;) {
            if(stopExceptionFlag){
                break;
            }
            Boolean kafkaConsumerFlag = false;
            for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
                if (consumeThread != null && consumeThread.kafkaConsumerFlag) {
                    kafkaConsumerFlag = true;
                }
            }
            if (!kafkaConsumerFlag) {
                break;
            }
            if(System.currentTimeMillis()-startTime > stopTimeOut){
                stopWithTimeOUt();
                stopExceptionFlag = true;
            }
        }
        logger.info("kafka consumer closed");
        // 等待所有consumer的working线程关闭
        for (;;) {
            if(stopExceptionFlag){
                break;
            }
            Boolean processDataWorkingFlag = false;
            for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
                if (consumeThread != null && consumeThread.processDataWorker.workingFlag) {
                    processDataWorkingFlag = true;
                }
            }
            if (!processDataWorkingFlag) {
                break;
            }
            if(System.currentTimeMillis()-startTime > stopTimeOut){
                stopWithTimeOUt();
                stopExceptionFlag = true;
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
        KafkaCache.kafkaConsumerOffsetMaps.clear();
        KafkaCache.consumeThreadList.clear();
        KafkaCache.rebalancerListenerList.clear();
        closeSignal.shutdown();
    }

    private void stopWithTimeOUt() {
        logger.info("kafka polling/kafka consumer/process data worker closed with timeout");
        for (KafkaSubscribeConsumeThread consumeThread : KafkaCache.consumeThreadList) {
            if(consumeThread != null && consumeThread.processDataWorker != null){
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
    }

    public void destroy() {
        MysqlOffsetPersist.destoryFlag = true;
        stop();
        closeSignal.afterDestroyConsumer();
        logger.info("mysql start to shutdown");
        // 关闭mysql连接
        MysqlOffsetPersist.getInstance().shutdown();
    }

    public void destroy(long stopTimeOut) {
        MysqlOffsetPersist.destoryFlag = true;
        stop(stopTimeOut);
        closeSignal.afterDestroyConsumer();
        logger.info("mysql start to shutdown");
        // 关闭mysql连接
        MysqlOffsetPersist.getInstance().shutdown();
    }

}
