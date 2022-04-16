package com.zandro.interactivequerydemo.component;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class Producer {
	
	private final KafkaTemplate<Integer, String> template;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
	
	public void publish(int key, String value) {
		ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("words", key, value);
		LOGGER.info(String.format("Producing => { key: %d, value: %s }", record.key(), record.value()));
		template.send(record);
	}
	
	public void publishForTestTopic(int key, String value) {
		ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("testTopic", key, value);
		LOGGER.info("Producing to test topic => { key: {}, value: {} }", record.key(), record.value());
		template.send(record);
	}

}
