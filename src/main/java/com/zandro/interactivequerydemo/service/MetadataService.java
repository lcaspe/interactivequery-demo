package com.zandro.interactivequerydemo.service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import com.zandro.interactivequerydemo.model.HostStoreInfo;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class MetadataService {

	private final StreamsBuilderFactoryBean factoryBean;

	/**
	 * Get the metadata for all of the instances of this Kafka Streams application
	 * 
	 * @return List of {@link HostStoreInfo}
	 */
	public List<HostStoreInfo> streamsMetadata() {
		// Get metadata for all of the instances of this Kafka Streams application
		final Collection<StreamsMetadata> metadata = factoryBean.getKafkaStreams().allMetadata();
		return mapInstancesToHostStoreInfo(metadata);
	}

	/**
	 * Get the metadata for all instances of this Kafka Streams application that
	 * currently has the provided store.
	 * 
	 * @param store The store to locate
	 * @return List of {@link HostStoreInfo}
	 */
	public List<HostStoreInfo> streamsMetadataForStore(final String store) {
		// Get metadata for all of the instances of this Kafka Streams application
		// hosting the store
		final Collection<StreamsMetadata> metadata = factoryBean.getKafkaStreams().allMetadataForStore(store);
		return mapInstancesToHostStoreInfo(metadata);
	}

	/**
	 * Find the metadata for the instance of this Kafka Streams Application that has
	 * the given store and would have the given key if it exists.
	 * 
	 * @param store Store to find
	 * @param key   The key to find
	 * @return {@link HostStoreInfo}
	 */
	public <K> HostStoreInfo streamsMetadataForStoreAndKey(final String store, final K key,
			final Serializer<K> serializer) {
		// Get metadata for the instances of this Kafka Streams application hosting the
		// store and
		// potentially the value for key
		final KeyQueryMetadata metadata = factoryBean.getKafkaStreams().queryMetadataForKey(store, key, serializer);
		if (metadata == null) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Metadata is null.");
		}

		return new HostStoreInfo(metadata.activeHost().host(), metadata.activeHost().port(),
				Collections.singleton(store));
	}

	private List<HostStoreInfo> mapInstancesToHostStoreInfo(final Collection<StreamsMetadata> metadatas) {
		return metadatas.stream()
				.map(metadata -> new HostStoreInfo(metadata.host(), metadata.port(), metadata.stateStoreNames()))
				.collect(Collectors.toList());
	}

}
