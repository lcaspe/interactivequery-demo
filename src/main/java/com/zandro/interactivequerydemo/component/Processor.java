package com.zandro.interactivequerydemo.component;

import java.util.Arrays;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Processor {
	
	@Autowired
	public void process(StreamsBuilder builder) {
		final Serde<Integer> integerSerde = Serdes.Integer();
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		
		KeyValueBytesStoreSupplier stateStore = Stores.persistentKeyValueStore("counts"); //Stores.inMemoryKeyValueStore("counts");
		KStream<Integer, String> textLines = builder.stream("words", Consumed.with(integerSerde, stringSerde));
		KTable<String, Long> wordCounts = textLines
				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				.groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
				.count(Materialized.as(stateStore));
		wordCounts.toStream().to("streams-wordcount-output", Produced.with(stringSerde, longSerde));
	}

}
