package com.zandro.interactivequerydemo.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

@Configuration
public class AppConfig {
	
	@Value("${server.port}")
	private int port;
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public NewTopic words() {
		return TopicBuilder.name("words").partitions(6).replicas(3).build();
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kafkaStreamsConfiguration() throws UnknownHostException {
		return new KafkaStreamsConfiguration(Map.of(
			StreamsConfig.APPLICATION_ID_CONFIG, "iq-app", 
			StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers, 
			StreamsConfig.APPLICATION_SERVER_CONFIG, InetAddress.getLocalHost().getHostAddress() + ":" + port
		));
	}
	
	@Bean
	public HostInfo hostInfo() throws UnknownHostException {
		String host = InetAddress.getLocalHost().getHostAddress();
		return new HostInfo(host, port);
	}

}
