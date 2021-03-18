package cn.thinkingdata.kafka.util;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
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
                .withWaitStrategy(WaitStrategies.fixedWait(sleepMilliseconds,TimeUnit.MILLISECONDS))
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
}
