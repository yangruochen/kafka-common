package cn.thinkingdata.kafka.test;


import cn.thinkingdata.kafka.close.ScanTermMethod;
import cn.thinkingdata.kafka.consumer.KafkaSubscribeConsumer;
import cn.thinkingdata.kafka.consumer.NewIDataLineProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestMain {

    private static final Logger logger = LoggerFactory
            .getLogger(TestMain.class);

    static String jdbcUrl = "jdbc:mysql://ta1:3306/ta?autoReconnect=true&amp;useUnicode=true";
//    static String dataProcessNum = "3";
    static KafkaSubscribeConsumer consumers;


    public static void main(String[] args) {

        String brokerList = args[0];
        String kafkaClusterName = args[1];
        String topic = args[2];
        String consumerGroup = args[3];
        String processThreadNum = args[4];
        String flushOffsetSize = args[5];
        String flushInterval = args[6];
        final String dataProcessNum = args[7];
        String maxPartitionFetchBytes = args[8];

        URL url = TestMain.class.getResource("/log4j.properties");
        PropertyConfigurator.configure(url);


        NewIDataLineProcessor dataProcessor = new NewIDataLineProcessor() {

            ThreadPoolExecutor executorService = new ThreadPoolExecutor(Integer.parseInt(dataProcessNum), Integer.parseInt(dataProcessNum),
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(500));{
                executorService.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            }

            @Override
            public void processData(ConsumerRecord<String, String> consumerRecord) {
                executorService.submit(new ProcessDataThread(consumerRecord.key(), consumerRecord.value()));
            }

            @Override
            public void finishProcess() {
                // 关闭线程池
                if (executorService != null)
                    executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                        logger.warn("Timed out waiting for data process threads to shut down, exiting uncleanly");
                    }
                } catch (InterruptedException e) {
                    logger.warn("Interrupted during shutdown, exiting uncleanly");
                }
            }
        };

        Map<String, String> map = new HashMap<>();
        map.put("jdbc.url", jdbcUrl);
        map.put("username", "ta");
        map.put("password", "ThinkingData2018");
        map.put("table.name", "kafka_consumer_offset");
        map.put("broker.list", brokerList);
        map.put("kafka.cluster.name", kafkaClusterName);
        map.put("topic", topic);
        map.put("consumer.group", consumerGroup);
        map.put("process.thread.num", processThreadNum);
        map.put("flush.offset.size", flushOffsetSize);
        map.put("flush.interval", flushInterval);
        if (maxPartitionFetchBytes != null) {
            map.put("max.partition.fetch.bytes", maxPartitionFetchBytes);
        }
        consumers = new KafkaSubscribeConsumer(map, dataProcessor, new ScanTermMethod());

        consumers.run();
    }
}
