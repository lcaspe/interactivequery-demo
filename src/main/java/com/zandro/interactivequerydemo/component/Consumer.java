package com.zandro.interactivequerydemo.component;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class Consumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

	@KafkaListener(topics = { "streams-wordcount-output" }, groupId = "kafka-streams-iq")
	public void consume(ConsumerRecord<String, Long> record) {
		LOGGER.info(String.format("Consuming => { key: %s value: %d }", record.key(), record.value()));
	}

}
