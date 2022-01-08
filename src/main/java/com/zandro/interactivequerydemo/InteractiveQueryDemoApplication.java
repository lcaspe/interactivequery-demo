package com.zandro.interactivequerydemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class InteractiveQueryDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(InteractiveQueryDemoApplication.class, args);
	}

}
