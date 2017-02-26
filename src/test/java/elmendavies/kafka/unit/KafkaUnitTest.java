package elmendavies.kafka.unit;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import scala.UninitializedError;

public class KafkaUnitTest {

	@Test
	public void testEphemeral() throws Exception {
		final KafkaUnit unit = new KafkaUnit();
		unit.startup();
		try {			
			unit.createTopic("test");			
			
			
			KafkaUnitProducer<String, String> producer = new KafkaUnitProducer<>(unit);
			producer.send(new ProducerRecord<String, String>("test", "Hola mundo"));
			
			KafkaUnitConsumer<String, String> consumer = new KafkaUnitConsumer<>(unit, Arrays.asList("test"));
			ConsumerRecord<String, String> record = consumer.read();
			assertEquals("Hola mundo", record.value());
		} finally {
			unit.shutdown();
		}
	}

}
