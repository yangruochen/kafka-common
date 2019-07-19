package cn.thinkingdata.kafka.util;

import com.github.rholder.retry.*;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by yangruchen on 2019/1/10.
 */
public class RetryerUtil {
    private static final Logger logger = LoggerFactory.getLogger(RetryerUtil.class);

    public static Retryer initRetryerByTimesWithIfResult(int retryTimes,long sleepMilliseconds, Predicate predicate){
        Retryer retryer = RetryerBuilder.newBuilder().retryIfException().retryIfResult(predicate)
                .withWaitStrategy(WaitStrategies.fixedWait(sleepMilliseconds, TimeUnit.MILLISECONDS))
                .withStopStrategy(StopStrategies.stopAfterAttempt(retryTimes))
                .withRetryListener(new RetryListener() {
                    @Override
                    public <V> void onRetry(Attempt<V> attempt) {
                        if (attempt.hasException()){
                            logger.error(Throwables.getStackTraceAsString(attempt.getExceptionCause()));
                        }
                        if(attempt.getAttemptNumber() > 1L){
                        	logger.info("开始进行失败重试，重试次数：" + attempt.getAttemptNumber() + "， 距离第一次失败时间：" + attempt.getDelaySinceFirstAttempt() + "毫秒");
                        }
                    }
                })
                .build();
        return retryer;
    }
    
//    public static Retryer initRetryerWithIfResult(Predicate predicate){
//        Retryer retryer = RetryerBuilder.newBuilder().retryIfException().retryIfResult(predicate)
//                .withWaitStrategy(WaitStrategies.fibonacciWait(500,5, TimeUnit.MINUTES))
//                .withStopStrategy(StopStrategies.neverStop())
//                .withRetryListener(new RetryListener() {
//                    @Override
//                    public <V> void onRetry(Attempt<V> attempt) {
//                        if (attempt.hasException()){
//                            logger.error(Throwables.getStackTraceAsString(attempt.getExceptionCause()));
//                        }
//                        logger.info("开始进行失败重试，重试次数：" + attempt.getAttemptNumber() + "， 距离第一次失败时间：" + attempt.getDelaySinceFirstAttempt() + "毫秒");
//                    }
//                })
//                .build();
//        return retryer;
//    }
    
    public static Retryer initRetryerWithStopTimeIfResult(long stopInSecond, Predicate predicate){
        Retryer retryer = RetryerBuilder.newBuilder().retryIfException().retryIfResult(predicate)
                .withWaitStrategy(WaitStrategies.fibonacciWait(500,1, TimeUnit.MINUTES))
                .withStopStrategy(StopStrategies.stopAfterDelay(stopInSecond, TimeUnit.SECONDS))
                .withRetryListener(new RetryListener() {
                    @Override
                    public <V> void onRetry(Attempt<V> attempt) {
                        if (attempt.hasException()){
                            logger.error(Throwables.getStackTraceAsString(attempt.getExceptionCause()));
                        }
                        logger.info("开始进行失败重试，重试次数：" + attempt.getAttemptNumber() + "， 距离第一次失败时间：" + attempt.getDelaySinceFirstAttempt() + "毫秒");
                    }
                })
                .build();
        return retryer;
    }
}
