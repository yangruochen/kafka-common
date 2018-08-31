package cn.thinkingdata.kafka.close;

public interface TermMethod {
	
	public Boolean receiveTermSignal();
	
	public void afterDestroyConsumer();

}
