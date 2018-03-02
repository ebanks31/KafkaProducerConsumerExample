package com.example.kafka.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * The Class KafkaConsumerExample.
 */
public class KafkaConsumerExample {

    /** The Constant TOPIC. */
    private final static String TOPIC = "my-example-topic";

    /** The Constant BOOTSTRAP_SERVERS. */
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String... args) throws Exception {
		runConsumer();
	}

	/**
	 * Run consumer.
	 *
	 * @throws InterruptedException the interrupted exception
	 */
	static void runConsumer() throws InterruptedException {
		final Consumer<Long, String> consumer = createConsumer();

		final int giveUp = 100;
		int noRecordsCount = 0;

		List<String> recordList = new ArrayList<String>();

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
						record.partition(), record.offset());

				recordList.add(String.format("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
						record.partition(), record.offset()));
			});

			consumer.commitAsync();
		}
		consumer.close();
		System.out.println("DONE");
		System.out.println("recordList: " + recordList);

	}

	  /**
  	 * Creates the consumer.
  	 *
  	 * @return the consumer
  	 */
  	private static Consumer<Long, String> createConsumer() {
	      final Properties props = new Properties();
	      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
	                                  BOOTSTRAP_SERVERS);
	      props.put(ConsumerConfig.GROUP_ID_CONFIG,
	                                  "KafkaExampleConsumer");
	      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	              LongDeserializer.class.getName());
	      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	              StringDeserializer.class.getName());

	      // Create the consumer using props.
	      final Consumer<Long, String> consumer =
	                                  new KafkaConsumer<>(props);

	      // Subscribe to the topic.
	      consumer.subscribe(Collections.singletonList(TOPIC));
	      return consumer;
	  }
}
