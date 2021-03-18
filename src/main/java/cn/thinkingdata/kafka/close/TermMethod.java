package cn.thinkingdata.kafka.close;

public interface TermMethod {

    Boolean receiveTermSignal();

    void afterDestroyConsumer();

}
