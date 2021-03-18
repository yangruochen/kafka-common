package cn.thinkingdata.kafka.close;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class SignalTermMethod implements TermMethod, SignalHandler {
    private static final Logger logger = LoggerFactory.getLogger(SignalTermMethod.class);

    Signal termSignal = new Signal("TERM");

    public Boolean termSignalFlag = false;

    public SignalTermMethod() {
        init();
    }

    public void init() {
        Signal.handle(termSignal, this);
    }

    @Override
    public Boolean receiveTermSignal() {
        return termSignalFlag;
    }

    @Override
    public void afterDestroyConsumer() {
    }

    @Override
    public void handle(Signal signal) {
        if (signal.getName().equals("TERM")) {
            logger.info("signal term received");
            termSignalFlag = true;
        }
    }

}
