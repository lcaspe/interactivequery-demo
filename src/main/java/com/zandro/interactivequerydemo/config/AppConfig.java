package com.zandro.interactivequerydemo.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.web.client.RestTemplate;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

@Configuration
public class AppConfig {
	
	@Value("${server.port}")
	private int port;

	@Bean
	public NewTopic words() {
		return TopicBuilder.name("words").partitions(6).replicas(1).build();
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kafkaStreamsConfiguration(KafkaProperties properties) throws UnknownHostException {
		return new KafkaStreamsConfiguration(Map.of(
			StreamsConfig.APPLICATION_ID_CONFIG, properties.getStreams().getApplicationId(), 
			StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers().size() > 0
							? properties.getBootstrapServers().get(0)
							: "localhost:9092", 
			StreamsConfig.APPLICATION_SERVER_CONFIG, InetAddress.getLocalHost().getHostAddress() + ":" + port
		));
	}
	
	@Bean
	public HostInfo hostInfo() throws UnknownHostException {
		return new HostInfo(InetAddress.getLocalHost().getHostAddress(), port);
	}
	
	@Bean
	public RestTemplate restTemplate(RestTemplateBuilder builder) {
		return builder.build();
	}

}
