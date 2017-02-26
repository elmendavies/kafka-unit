package elmendavies.kafka.unit;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaUnitConsumer<K, T> {
	private final KafkaUnit unit;
	private final KafkaConsumer<K,T> consumer;
	private Iterator<ConsumerRecord<K, T>> iterator;
	
	public KafkaUnitConsumer(KafkaUnit unit, Collection<String> topics) {
		this.unit = unit;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", unit.getKafkaConnect());
		props.put("group.id", "consumer-kafka-unit");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("auto.offset.reset", "earliest");
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(topics);
	}
	
	public ConsumerRecord<K, T> read() {
		while (iterator == null || ! iterator.hasNext()) {
			ConsumerRecords<K, T> records = (ConsumerRecords<K, T>) consumer.poll(1000);
			iterator = records.iterator();
		}
		return iterator.next();
	}
	
}
