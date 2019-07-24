package cn.thinkingdata.kafka.consumer;

import cn.thinkingdata.kafka.cache.KafkaCache;
import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.consumer.dao.KafkaConsumerOffset;
import cn.thinkingdata.kafka.consumer.offset.MysqlOffsetManager;
import cn.thinkingdata.kafka.consumer.offset.OffsetManager;
import cn.thinkingdata.kafka.consumer.persist.MysqlOffsetPersist;
import cn.thinkingdata.kafka.util.CommonUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class KafkaSubscribeConsumeThread implements Runnable {

    private static final Logger logger = LoggerFactory
            .getLogger(KafkaSubscribeConsumeThread.class);
    private IDataLineProcessor dataProcessor;
    public KafkaConsumer<String, String> consumer;
    private OffsetManager offsetManager = MysqlOffsetManager.getInstance();
    public volatile Boolean kafkaPollFlag = false;
    public volatile Boolean kafkaConsumerFlag = false;
    // public volatile Boolean offsetFlushFlag = false;
    private CyclicBarrier offsetFlushBarrier;
    public volatile Collection<TopicPartition> assignedPartitions = null;
    private BlockingQueue<ConsumerRecord<String, String>> unsent = new LinkedBlockingQueue<>();
    public Set<KafkaConsumerOffset> kafkaConsumerOffsetSet = new HashSet<KafkaConsumerOffset>();
    // records的初始值是1000，所以这边capacity的值设为2000
    private final BlockingQueue<ConsumerRecord<String, String>> processDataQueue =
            new LinkedBlockingQueue<>(3000);
    private volatile Thread consumerThread;
    public ProcessDataWorker processDataWorker = new ProcessDataWorker();


    /**
     * The consumer is currently paused due to a slow execution data. The consumer will be
     * resumed when the current batch of records has been processed but will continue
     * to be polled.
     */
    private volatile Boolean paused = false;

    public KafkaSubscribeConsumeThread(KafkaConsumer<String, String> consumer,
                                       IDataLineProcessor dataProcessor, CyclicBarrier offsetFlushBarrier) {
        this.consumer = consumer;
        this.dataProcessor = dataProcessor;
        this.offsetFlushBarrier = offsetFlushBarrier;
    }

    @Override
    public void run() {
        consumerThread = Thread.currentThread();
        kafkaConsumerFlag = true;
        //启动processDataWorker
        new Thread(processDataWorker, consumerThread.getName() + "-" + "working thread").start();
        Set<ConsumerRecord<String, String>> lastConsumerRecordSet = new HashSet<ConsumerRecord<String, String>>();
        Long count = 0L;
        DateTime sessionTimeoutDataTime = new DateTime().plusSeconds(Integer
                .parseInt(KafkaMysqlOffsetParameter.sessionTimeout));
        try {
            while (!KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed
                    .get()) {
                if (KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState
                        .get()) {
                    kafkaPollFlag = true;
                    count = 0L;
                    // 如果执行dataExecute的时间超过了sessionTimeout
                    if (sessionTimeoutDataTime.isAfterNow()) {
                        // 如果有新的consumer，则调用rebalance，并阻塞线程
                        ConsumerRecords<String, String> records = null;
                        try {
                            records = consumer
                                    .poll(KafkaMysqlOffsetParameter.pollInterval * 1000);
                        } catch (OffsetOutOfRangeException e) {
                            synchronized (OffsetManager.class){
                                MysqlOffsetManager.getInstance().getExternalStorePersist().executeWhenOffsetReset();
                            }
                        }
                        // 计算开始时间
                        sessionTimeoutDataTime = new DateTime()
                                .plusSeconds(Integer
                                        .parseInt(KafkaMysqlOffsetParameter.sessionTimeout) / 1000);
                        logger.debug("sessionTimeoutDataTime is " + sessionTimeoutDataTime.toString());
                        if(records != null){
                            if (records.count() > 0) {
                                logger.debug("poll records size: " + records.count()
                                        + ", partition is " + records.partitions()
                                        + ", thread is "
                                        + Thread.currentThread().getName());
                            }
                            //先暂停
                            pause();
                            //放到队列
                            sendToQueue(records);
                            //恢复
                            if (isResume()) {
                                resume();
                            } else {
                                // 休息3秒
                                Thread.sleep(3000);
                            }
                            //获取LastConsumerRecord
                            for (ConsumerRecord<String, String> consumerRecord : records) {
                                count++;
    //                            dataProcessor.processData(consumerRecord.key(), consumerRecord.value());
                                addLastConsumerRecord(lastConsumerRecordSet,
                                        consumerRecord);
                            }
                            // 更新offset
                            saveLastConsumerRecordSet(lastConsumerRecordSet,count,false);
                            lastConsumerRecordSet.clear();
                        }
                    } else {
                        // sessionTimeOut了，进行异常处理
                        logger.info("kafka session time out, the consumer is " + consumer.toString());
                        MysqlOffsetManager.getInstance()
                                .getExternalStorePersist()
                                .executeWhenExecuteDataSessionTimeout(this);
                        break;
                    }
                } else {
                    logger.info("mysql and backup store connect error, the mysqlAndBackupStoreConnState is "
                            + KafkaMysqlOffsetParameter.mysqlAndBackupStoreConnState
                            .get());
                    kafkaPollFlag = false;
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        logger.error("------- thread can not sleep ---------------------"
                                + e.toString());
                    }
                }
            }
            kafkaPollFlag = false;
            logger.info("kafka consumer close, the kafkaSubscribeConsumerClosed is "
                    + KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed
                    .get());
        } catch (WakeupException | InterruptedException e) {
            // 外部thread中断kafka的poll操作
            logger.info("stop consumer with wakeup or interupted, the kafkaSubscribeConsumerClosed is "
                    + KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed
                    .get()
                    + ", the thread is "
                    + Thread.currentThread().getName()
                    + ", the consumeThreadList is "
                    + StringUtils.join(KafkaCache.consumeThreadList.stream().map(o->o.consumerThread.getName()).collect(Collectors.toList()),",")
                    + " the kafka cluster name is "
                    + KafkaMysqlOffsetParameter.kafkaClusterName
                    + ", the Exception is " + CommonUtils.getStackTraceAsString(e));
            // 更新offset
            saveLastConsumerRecordSet(lastConsumerRecordSet,count,true);
            kafkaPollFlag = false;
            logger.info("stop consumer with wakeup finished");
        } catch(Exception e){
            // 更新offset
            saveLastConsumerRecordSet(lastConsumerRecordSet,count,false);
            logger.error("stop consumer with exception, the kafkaSubscribeConsumerClosed is "
                    + KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed
                    .get()
                    + ", the thread is "
                    + Thread.currentThread().getName()
                    + ", the consumeThreadList is "
                    + StringUtils.join(KafkaCache.consumeThreadList.stream().map(o->o.consumerThread.getName()).collect(Collectors.toList()),",")
                    + " the kafka cluster name is "
                    + KafkaMysqlOffsetParameter.kafkaClusterName
                    + ", the kafkaConsumerOffsets In cache is" + KafkaCache.kafkaConsumerOffsets + ", the Exception is " + CommonUtils.getStackTraceAsString(e));
            kafkaPollFlag = false;
            logger.info("stop consumer finished");
            synchronized (OffsetManager.class){
                MysqlOffsetManager.getInstance().getExternalStorePersist().executeWhenException();
            }
        } finally {
            try {
                closeKafkaSubscribeConsumeThread();
            } catch (Exception e) {
                logger.error("closeKafkaSubscribeConsumeThread error, the thread is " + Thread.currentThread().getName() + ", the Exception is " + CommonUtils.getStackTraceAsString(e));
            }
        }
    }

    private void saveLastConsumerRecordSet(Set<ConsumerRecord<String, String>> lastConsumerRecordSet, Long count, Boolean cleanOwner){
        for (ConsumerRecord<String, String> lastConsumerRecord : lastConsumerRecordSet) {
            Date now = new Date();
            KafkaConsumerOffset kafkaConsumerOffset = KafkaCache
                    .searchKafkaConsumerOffset(
                            lastConsumerRecord.topic(),
                            lastConsumerRecord.partition());
            if (kafkaConsumerOffset == null) {
                logger.error("kafkaConsumerOffset is null in cache, the lastConsumerRecord is "
                        + lastConsumerRecord
                        + ", the kafkaConsumerOffsets is "
                        + KafkaCache.kafkaConsumerOffsets);
                kafkaConsumerOffset = offsetManager.readOffsetFromCache(
                        lastConsumerRecord.topic(),
                        lastConsumerRecord.partition());
                // 设定owner
                kafkaConsumerOffset
                        .setOwner(KafkaMysqlOffsetParameter.kafkaClusterName
                                + "-"
                                + lastConsumerRecord.topic()
                                + "-"
                                + lastConsumerRecord.partition()
                                + "-"
                                + KafkaMysqlOffsetParameter.consumerGroup
                                + "-"
                                + now.getTime()
                                + "-"
                                + KafkaMysqlOffsetParameter.hostname
                                + "-"
                                + consumer.toString().substring(
                                consumer.toString()
                                        .lastIndexOf("@") + 1));
            }
            kafkaConsumerOffset.setTopic(lastConsumerRecord.topic());
            kafkaConsumerOffset.setPartition(lastConsumerRecord.partition());
            kafkaConsumerOffset
                    .setConsumer_group(KafkaMysqlOffsetParameter.consumerGroup);
            kafkaConsumerOffset.setOffset(lastConsumerRecord.offset() + 1L);
            kafkaConsumerOffset
                    .setKafka_cluster_name(KafkaMysqlOffsetParameter.kafkaClusterName);
            kafkaConsumerOffset.setCount(count);
            if(cleanOwner){
                //退出的时候清除Owner
                kafkaConsumerOffset.setOwner("");
                logger.info("clean owner, the thread is "+ Thread.currentThread().getName()+ ", the kafkaConsumerOffset is "+ kafkaConsumerOffset.toString());
            }
            kafkaConsumerOffsetSet.add(kafkaConsumerOffset);
            offsetManager.saveOffsetInCache(kafkaConsumerOffset);
        }
    }

    public void closeKafkaSubscribeConsumeThread() throws InterruptedException {
        logger.info("start to stop processDataWorker " + processDataWorker.executingThread.getName());
        processDataWorker.stop();
        logger.info("wait for the mysql persist finish");
        // 等待MysqlOffsetPersist的persist动作完成
        for (; ; ) {
            if (!MysqlOffsetPersist.runFlag && KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed
                    .get()) {
                break;
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    logger.error("------- thread can not sleep ---------------------"
                            + e.toString());
                }
            }
        }
        logger.info("flush before kafka consumer close");
        logger.debug("kafkaConsumerOffsets is "
                + KafkaCache.kafkaConsumerOffsets);
        logger.debug("kafkaConsumerOffsetSet is " + kafkaConsumerOffsetSet
                + " ,the thread is " + Thread.currentThread().getName());
        try {
            for (KafkaConsumerOffset kafkaConsumerOffset : kafkaConsumerOffsetSet) {
                KafkaConsumerOffset kafkaConsumerOffsetInCache = KafkaCache
                        .searchKafkaConsumerOffset(
                                kafkaConsumerOffset.getTopic(),
                                kafkaConsumerOffset.getPartition());
                // 因为有Marking the coordinator
                // dead的情况，所以可能kafkaConsumerOffsetSet里有该partition，
                // 而另一个线程的kafkaConsumerOffsetSet也有该partition，前一个已经在KafkaCache.kafkaConsumerOffsets中remove了，
                // 所以有可能查出来是null
                if (kafkaConsumerOffsetInCache != null) {
                    TopicPartition topicPartition = new TopicPartition(
                            kafkaConsumerOffset.getTopic(),
                            kafkaConsumerOffset.getPartition());
                    // 因为有可能mysql里的kafka_consumer_offset为空，consumer拿lastest，这时候的offset不是0，是lastest，是需要保存的
                    Long consumerPosition = null;
                    try {
                        consumerPosition = consumer
                                .position(topicPartition);
                    } catch (Exception e) {
                        logger.info("the consumer get position error, the error is "
                                + e.toString()
                                + ", the topicPartition is "
                                + topicPartition);
                    }
                    logger.debug("consumer position is " + consumerPosition);
                    if (consumerPosition != null
                            && consumerPosition != 0L
                            && kafkaConsumerOffsetInCache != null
                            && consumerPosition > kafkaConsumerOffsetInCache
                            .getOffset()) {
                        logger.info("consumer position "
                                + consumerPosition
                                + "is bigger than the offset in kafkaConsumerOffsetInCache "
                                + kafkaConsumerOffsetInCache);
                        kafkaConsumerOffsetInCache
                                .setOffset(consumerPosition);
                    }
                    MysqlOffsetPersist.getInstance().flush(
                            kafkaConsumerOffsetInCache);
                } else {
                    logger.error("kafkaConsumerOffsetInCache is null, kafkaConsumerOffset is "
                            + kafkaConsumerOffset
                            + ", kafkaConsumerOffsetSet is "
                            + kafkaConsumerOffsetSet
                            + ", kafkaConsumerOffsets is "
                            + KafkaCache.kafkaConsumerOffsets);
                }
            }
            // 之前的kafkaConsumerOffsetSet可能为空，所以需要最后测算一下kafkaConsumerOffsets里是否有空余的
            // offsetFlushFlag = true;
            // for (;;) {
            // Boolean allOffsetFlushFlag = true;
            // for (KafkaSubscribeConsumeThread consumeThread :
            // KafkaCache.consumeThreadList) {
            // if (consumeThread != null
            // && !consumeThread.offsetFlushFlag)
            // allOffsetFlushFlag = false;
            // }
            // if (allOffsetFlushFlag)
            // break;
            // }
            offsetFlushBarrier.await();
            logger.info("start to flush the rest KafkaCache.kafkaConsumerOffsets "
                    + KafkaCache.kafkaConsumerOffsets
                    + ", the thread is "
                    + Thread.currentThread().getName());
            flushKafkaConsumerOffsetsInKafkaCache();
            consumer.close();
        } catch (Exception e) {
            logger.error("close consumer error, the exception is "
                    + CommonUtils.getStackTraceAsString(e));
            consumer.close();
        }
        kafkaConsumerFlag = false;
        sendUnsentToProcessDataQueue(true);
        logger.info("kafka consumer finally close");
    }

    private KafkaConsumerOffset searchKafkaConsumerOffsetSet(
            Set<KafkaConsumerOffset> kafkaConsumerOffsetSet,
            KafkaConsumerOffset kafkaConsumerOffset) {
        for (KafkaConsumerOffset KafkaConsumerOffsetInSet : kafkaConsumerOffsetSet) {
            if (KafkaConsumerOffsetInSet.getTopic().equals(
                    kafkaConsumerOffset.getTopic())
                    && KafkaConsumerOffsetInSet.getPartition().equals(kafkaConsumerOffset
                    .getPartition())) {
                return KafkaConsumerOffsetInSet;
            }
        }
        return null;
    }

    private synchronized void flushKafkaConsumerOffsetsInKafkaCache() {

        for (KafkaConsumerOffset kafkaConsumerOffset : KafkaCache.kafkaConsumerOffsets) {
            logger.info("kafkaConsumerOffset in cache is not be consumed, kafkaConsumerOffset is "
                    + kafkaConsumerOffset);
            // 因为有可能mysql里的kafka_consumer_offset为空，consumer拿lastest，这时候的offset不是0，是lastest，是需要保存的
            // TopicPartition topicPartition = new TopicPartition(
            // kafkaConsumerOffset.getTopic(),
            // kafkaConsumerOffset.getPartition());
            // Long consumerPosition = consumer.position(topicPartition);

            TopicPartition topicPartition = new TopicPartition(
                    kafkaConsumerOffset.getTopic(),
                    kafkaConsumerOffset.getPartition());
            Long consumerPosition = null;
            try {
                consumerPosition = consumer.position(topicPartition);
            } catch (IllegalArgumentException e) {
                logger.info("flushKafkaConsumerOffsetsInKafkaCache, the consumer get position error, the error is "
                        + e.toString()
                        + ", the topicPartition is "
                        + topicPartition);
            }
            if (kafkaConsumerOffset != null) {
                if (consumerPosition != null && consumerPosition != 0L
                        && consumerPosition > kafkaConsumerOffset.getOffset()) {
                    logger.debug("consumer position is " + consumerPosition);
                    logger.info("consumer position " + consumerPosition
                            + " is bigger than the offset in kafkaConsumerOffset "
                            + kafkaConsumerOffset);
                    kafkaConsumerOffset.setOffset(consumerPosition);
                }
                MysqlOffsetPersist.getInstance().flush(kafkaConsumerOffset);
            }
        }
    }

    private void addLastConsumerRecord(
            Set<ConsumerRecord<String, String>> lastConsumerRecordSet,
            ConsumerRecord<String, String> consumerRecord) {
        ConsumerRecord<String, String> consumerRecordInSet = searchConsumerRecord(
                lastConsumerRecordSet, consumerRecord);
        if (consumerRecordInSet == null) {
            lastConsumerRecordSet.add(consumerRecord);
        } else {
            lastConsumerRecordSet.remove(consumerRecordInSet);
            lastConsumerRecordSet.add(consumerRecord);
        }
    }

    private ConsumerRecord<String, String> searchConsumerRecord(
            Set<ConsumerRecord<String, String>> lastConsumerRecordSet,
            ConsumerRecord<String, String> consumerRecord) {
        for (ConsumerRecord<String, String> consumerRecordInSet : lastConsumerRecordSet) {
            if (consumerRecordInSet.topic().equals(consumerRecord.topic())
                    && consumerRecordInSet.partition() == consumerRecord
                    .partition()) {
                return consumerRecordInSet;
            }
        }
        return null;
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        KafkaMysqlOffsetParameter.kafkaSubscribeConsumerClosed.set(true);
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    public void pause() {
        if (this.assignedPartitions != null) {
            // avoid group management rebalance due to a slow
            // consumer
            this.consumer.pause(this.assignedPartitions);
            this.paused = true;
        }
    }

    public Boolean isResume() {
        // 如果unsent为空，则恢复消费
        return CollectionUtils.isEmpty(unsent);
    }

    public void resume() {
        if (this.assignedPartitions != null) {
            // avoid group management rebalance due to a slow
            // consumer
            this.consumer.resume(this.assignedPartitions);
            this.paused = false;
        }
    }

    private void sendToQueue(ConsumerRecords<String, String> records) throws InterruptedException {
        Boolean flag = true;
        if (CollectionUtils.isEmpty(unsent)) {
            for (ConsumerRecord<String, String> record : records) {
                if (flag) {
                    flag = this.processDataQueue.offer(record, 200, TimeUnit.MILLISECONDS);
                    //如果没有放入成功说明队列已满
                    if (!flag) {
                        logger.info("the processDataQueue is full...");
                        unsent.put(record);
                    }
                } else {
                    unsent.put(record);
                }
            }
        } else {
            if(records.count()>0){
                logger.info("the unsent is not empty but the consummer still polling records, it can be only happed after rebalanced");
            }
            for (ConsumerRecord<String, String> record : records) {
                unsent.put(record);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error("------- thread can not sleep ---------------------"
                        + e.toString());
            }
            // 试着将unsent里的records放入processDataQueue
            sendUnsentToProcessDataQueue(false);
        }
    }

    private void sendUnsentToProcessDataQueue(Boolean shutdown) throws InterruptedException {
        while (CollectionUtils.isNotEmpty(unsent)) {
            //拿出队首元素但不出栈
            ConsumerRecord<String, String> recordInUnsent = unsent.peek();
            if(recordInUnsent != null){
                Boolean flag = this.processDataQueue.offer(recordInUnsent, 200, TimeUnit.MILLISECONDS);
                if (!flag) {
                    //如果没有放入processDataQueue成功说明队列已满
                    logger.info("the processDataQueue is full... and the unsent is not empty");
                    //如果没有停止，则跳出，否则需要将unsent清空才能退出
                    if(!shutdown){
                        break;
                    } else {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            logger.error("------- thread can not sleep ---------------------"
                                    + e.toString());
                        }
                    }
                } else {
                    //如果放入processDataQueue成功则出栈
                    unsent.poll();
                }
            } else {
                logger.error("the unsent is not empty, but the recordInUnsent is null!!");
            }
        }
    }

    //processData线程消费queue
    public final class ProcessDataWorker implements Runnable {

        private final CountDownLatch exitLatch = new CountDownLatch(1);
        private volatile Thread executingThread;
        private volatile Boolean workerStopFlag = false;
        public volatile Boolean workingFlag = false;
        private volatile ConsumerRecord<String, String> consumerRecord;
        private static final long MAX_WAIT_MS = 1000;

        @Override
        public void run() {
            workingFlag = true;
            try {
                this.executingThread = Thread.currentThread();
                while (true) {
                    processOperationData();
                    // 如果queue是空，并且stop为true则退出
                    if(processDataQueue.size() == 0 && workerStopFlag && unsent.size() == 0) {
                        break;
                    }
                }
//            } catch (InterruptedException e) {
//                logger.info("processDataWorker thread is interrupted");
//                // 现在先把queue清空
//                while (processDataQueue.size() != 0) {
//                    try {
//                        processOperationData();
//                    } catch (InterruptedException e1) {
//                        logger.error("processDataWorker thread is abnormal interrupted in stopping, the error is "
//                                + e1.toString());
//                    } catch (Exception e1) {
//                        logger.error("processDataWorker thread is failed in stopping, the error is "
//                                + e1.toString());
//                    }
//                }
            } catch (Exception e) {
                logger.error("processDataWorker thread is failed, the error is "
                        + e.toString());
            } finally {
                logger.info("processDataWorker " + Thread.currentThread().getName() + " is safely closed...");
                exitLatch.countDown();
                workingFlag = false;
            }
        }

        private void processOperationData() throws InterruptedException {
            consumerRecord = processDataQueue.poll(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
            if (consumerRecord != null) {
                dataProcessor.processData(consumerRecord.key(), consumerRecord.value());
            }
        }

        public void stopWithException() {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("------- thread can not sleep ---------------------"
                        + e.toString());
            }
            workerStopFlag = true;
        }


        private void stop() {
//            try {
            // 等待拉取动作结束
            for (; ; ) {
                if (kafkaPollFlag == false) {
                    break;
                } else {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        logger.error("------- thread can not sleep ---------------------"
                                + e.toString());
                    }
                }
            }
//            this.executingThread.interrupt();
            workerStopFlag = true;
            //1分钟之后，强制退出
//                if (!this.exitLatch.await(60000, TimeUnit.MILLISECONDS)
//                        && this.executingThread != null) {
//                    this.executingThread.interrupt();
//                }
//            }
//            catch (InterruptedException e) {
//                logger.error("processDataWorker stop thread is abnormal interrputed, the error is "
//                        + e.toString());
//                if (this.executingThread != null) {
//                    this.executingThread.interrupt();
//                }
//            } finally {
            //TODO 二期queue和unsent没有清空数据持久化到磁盘
//                if(unsent.size() > 0){
//                    writeUnsentToDisk(unsent);
//                }
//                if(processDataQueue.size() > 0){
//                    writeProcessDataQueueToDisk(processDataQueue);
//                }
//            }
        }

//        private void writeProcessDataQueueToDisk(BlockingQueue<ConsumerRecord<String, String>> processDataQueue) {
//            String fileName = getWriteDiskFileName("processDataQueue");
//
//        }
//
//        private void writeUnsentToDisk(List<ConsumerRecord<String, String>> unsentList) {
//            String fileName = getWriteDiskFileName("unsent");
//
//        }
    }
}



