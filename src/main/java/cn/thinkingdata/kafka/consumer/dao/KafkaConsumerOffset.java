package cn.thinkingdata.kafka.consumer.dao;

import java.util.Date;

public class KafkaConsumerOffset {
	private Integer oid;
	private String topic;
	private Integer partition;
	private String consumer_group;
	private Long offset;
	// 上一次flush记录下的offset值
	private Long last_flush_offset;
	// 计数器 每消费一个记录，计数器加一，flush完，计数器清零
	private Long count;
	private String kafka_cluster_name;
	private String owner;
	private Date update_time;
	private Date create_time;

	public Integer getOid() {
		return oid;
	}

	public void setOid(Integer oid) {
		this.oid = oid;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public String getConsumer_group() {
		return consumer_group;
	}

	public void setConsumer_group(String consumer_group) {
		this.consumer_group = consumer_group;
	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public Long getLast_flush_offset() {
		return last_flush_offset;
	}

	public void setLast_flush_offset(Long last_flush_offset) {
		this.last_flush_offset = last_flush_offset;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public String getKafka_cluster_name() {
		return kafka_cluster_name;
	}

	public void setKafka_cluster_name(String kafka_cluster_name) {
		this.kafka_cluster_name = kafka_cluster_name;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public Date getUpdate_time() {
		return update_time;
	}

	public void setUpdate_time(Date update_time) {
		this.update_time = update_time;
	}

	public Date getCreate_time() {
		return create_time;
	}

	public void setCreate_time(Date create_time) {
		this.create_time = create_time;
	}

	@Override
	public String toString() {
		return "KafkaConsumerOffset [oid=" + oid + ", topic=" + topic
				+ ", partition=" + partition + ", consumer_group="
				+ consumer_group + ", offset=" + offset
				+ ", last_flush_offset=" + last_flush_offset + ", count="
				+ count + ", kafka_cluster_name=" + kafka_cluster_name
				+ ", owner=" + owner + ", update_time=" + update_time
				+ ", create_time=" + create_time + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((consumer_group == null) ? 0 : consumer_group.hashCode());
		result = prime
				* result
				+ ((kafka_cluster_name == null) ? 0 : kafka_cluster_name
						.hashCode());
		result = prime * result
				+ ((partition == null) ? 0 : partition.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KafkaConsumerOffset other = (KafkaConsumerOffset) obj;
		if (consumer_group == null) {
			if (other.consumer_group != null)
				return false;
		} else if (!consumer_group.equals(other.consumer_group))
			return false;
		if (kafka_cluster_name == null) {
			if (other.kafka_cluster_name != null)
				return false;
		} else if (!kafka_cluster_name.equals(other.kafka_cluster_name))
			return false;
		if (partition == null) {
			if (other.partition != null)
				return false;
		} else if (!partition.equals(other.partition))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}

	

}
