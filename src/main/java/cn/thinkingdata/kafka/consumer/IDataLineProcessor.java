package cn.thinkingdata.kafka.consumer;

public interface IDataLineProcessor {
	public void processData(String key,String vaule);
	
	public void finishProcess();
}
