package com.zandro.interactivequerydemo.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.zandro.interactivequerydemo.model.KeyValueBean;

@EmbeddedKafka(partitions = 6, controlledShutdown = false, brokerProperties = {
		"listeners=PLAINTEXT://localhost:9092" })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
public class QueryServiceTest {

	@Autowired
	private QueryService underTest;
	
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	
	private Producer<Integer, String> producer;
	
	
	@BeforeAll
	void setup() {
		Map<String, Object> producerConfigs = KafkaTestUtils.producerProps(embeddedKafkaBroker);
		producer = new DefaultKafkaProducerFactory<>(producerConfigs, new IntegerSerializer(), new StringSerializer())
				.createProducer();
	}
	
	@AfterAll
	void tearDown() {
		producer.close();
	}

	@Test
	public void testGetAllList() {
		producer.send(new ProducerRecord<Integer, String>("words", 1, "testword"));

		//Awaitility.waitAtMost(360, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {

			List<KeyValueBean> list = underTest.allForStore("counts");
			assertThat(list).isNotEmpty();
		//});
	}
	
	@Test
	public void thisTestShouldFail() {
		assertThat(4).isEqualTo(1 + 2);
	}

}
