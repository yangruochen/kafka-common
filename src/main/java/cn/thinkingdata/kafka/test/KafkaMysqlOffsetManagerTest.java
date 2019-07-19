package cn.thinkingdata.kafka.test;//package cn.thinkingdata.kafka.test;
//
//
//
//
//import cn.thinkingdata.kafka.close.TermMethod;
//import cn.thinkingdata.kafka.consumer.IDataLineProcessor;
//import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumer;
//import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.concurrent.RejectedExecutionHandler;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * Created by zhoujin on 2018/9/4.
// */
//public class KafkaMysqlOffsetManagerTest {
//    private static final String JDBC_URL = "jdbc:mysql://mysql-test:3306/ta?autoReconnect=true&amp;useUnicode=true";
//    private static final String MYSQL_USER = "ta";
//    private static final String MYSQL_PASSWORD = "TaThinkingData";
//    private static final String OFFSET_MANAGER_TABLE = "kafka_consumer_offset";
//    private static final String KAFKA_BROKERS = "app:9092,app2:9092";
//    private static final String KAFKA_CLUSTER_NAME = "thinkingdata-online-kafka";
//    private static final String TEST_TOPIC = "mysql-offset-topic-test";
//    private static final String CONSUMER_GROUP = "test-group";
//    private static final Integer CONSUMER_THREAD_NUM = 2;
//    private static final Integer FLUSH_BATCH_SIZE = 50000;
//    private static final Integer FLUSH_INTERVAL = 5;
////    private static final Long KAFKA_PARTITION_MAX_FETCH_BYTES = 524288L;
//
//    private static AtomicLong messageCount = new AtomicLong(0L);
//
//        private static ThreadPoolTaskExecutor executor = kafkaDataExecutor();
//
//    public static void main(String[] args) throws IOException {
//        KafkaSubscribeConsumer kafkaSubscribeConsumer = new KafkaSubscribeConsumer(JDBC_URL, MYSQL_USER, MYSQL_PASSWORD,
//                OFFSET_MANAGER_TABLE, KAFKA_BROKERS, KAFKA_CLUSTER_NAME, TEST_TOPIC,
//                CONSUMER_GROUP, new DataCountProcessor(), CONSUMER_THREAD_NUM,
//                FLUSH_BATCH_SIZE, FLUSH_INTERVAL, new TermMethod() {
//            @Override
//            public Boolean receiveTermSignal() {
//                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//                String line;
//                try {
//                    while ((line = reader.readLine()) != null) {
//                        if (line.length() > 0) {
//                            reader.close();
//                            return true;
//                        }
//
//                    }
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                return false;
//            }
//
//            @Override
//            public void afterDestroyConsumer() {
//                System.out.println("最终消费数量：" + messageCount);
//            }
//        });
//        kafkaSubscribeConsumer.run();
//    }
//
//    private static class DataCountProcessor implements IDataLineProcessor{
//
//        @Override
//        public void processData(String key, String value) {
//            executor.submit(new ProcessThread(value));
//        }
//
//        @Override
//        public void finishProcess() {
//            executor.shutdown();
//
//        }
//    }
//
//    private static class ProcessThread implements Runnable{
//        private String value;
//
//        public ProcessThread(String value) {
//            this.value = value;
//        }
//        @Override
//        public void run() {
//            long count = messageCount.incrementAndGet();
//            if(count % 10000 == 0){
//                System.out.println("当前线程：" + Thread.currentThread().getName() + "， 已处理消息数：" + count + "， 当前处理value: " + value);
//            }
//
//        }
//    }
//
//    private static ThreadPoolTaskExecutor kafkaDataExecutor() {
//        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
//        executor.setCorePoolSize(5);
//        executor.setMaxPoolSize(5);
//        executor.setQueueCapacity(500);
//        executor.setThreadNamePrefix("data-processor-thread-");
//        executor.setKeepAliveSeconds(0);
//        executor.setAwaitTerminationSeconds(5);
//        executor.setWaitForTasksToCompleteOnShutdown(true);
//        executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
//            @Override
//            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//                try {
//                    executor.getQueue().put(r);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        executor.initialize();
//        return executor;
//
//    }
//}
