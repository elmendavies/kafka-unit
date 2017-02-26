package elmendavies.kafka.unit;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import kafka.serializer.StringEncoder;

public class KafkaUnitProducer<K,T> {
	final private KafkaUnit unit;
	final private Producer<K,T> producer;

	public KafkaUnitProducer(KafkaUnit unit) {
		super();
		this.unit = unit;
        Properties props = new Properties();
        props.put("bootstrap.servers", unit.getKafkaConnect());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<K,T>(props);
	}
	
	public void send(ProducerRecord<K,T> record) {
		producer.send(record);
	}
	
	public void flush() {
		producer.flush();
	}
}
