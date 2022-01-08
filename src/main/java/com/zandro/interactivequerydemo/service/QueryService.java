package com.zandro.interactivequerydemo.service;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import com.zandro.interactivequerydemo.model.HostStoreInfo;
import com.zandro.interactivequerydemo.model.KeyValueBean;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class QueryService {

	private final StreamsBuilderFactoryBean factoryBean;
	private final MetadataService metadataService;
	private final HostInfo hostInfo;
	private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();

	/**
	 * Get a key-value pair from a KeyValue Store
	 * 
	 * @param storeName the store to look in
	 * @param key       the key to get
	 * @return {@link KeyValueBean} representing the key-value pair
	 */
	public KeyValueBean byKey(String storeName, String key) {
		final HostStoreInfo hostStoreInfo = streamsMetadataForStoreAndKey(storeName, key);
		if (!thisHost(hostStoreInfo)) {
			return fetchByKey(hostStoreInfo, "keyvalue/" + storeName + "/" + key);
		}

		// Lookup the KeyValueStore with the provided storeName
		final ReadOnlyKeyValueStore<String, Long> store = factoryBean.getKafkaStreams()
				.store(fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
		if (store == null) {
			throw new NotFoundException();
		}

		// Get the value from the store
		final Long value = store.get(key);
		if (value == null) {
			throw new NotFoundException();
		}

		return new KeyValueBean(key, value);
	}

	private KeyValueBean fetchByKey(final HostStoreInfo host, final String path) {
		return client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
				.request(MediaType.APPLICATION_JSON_TYPE).get(new GenericType<KeyValueBean>() {
				});
	}

	/**
	 * Get all of the key-value pairs available in a store
	 * 
	 * @param storeName store to query
	 * @return A List of {@link KeyValueBean}s representing all of the key-values in
	 *         the provided store
	 */
	public List<KeyValueBean> allForStore(String storeName) {
		return rangeForKeyValueStore(storeName, ReadOnlyKeyValueStore::all);
	}

	/**
	 * Get all of the key-value pairs that have keys within the range from...to
	 * 
	 * @param storeName store to query
	 * @param from      start of the range (inclusive)
	 * @param to        end of the range (inclusive)
	 * @return A List of {@link KeyValueBean}s representing all of the key-values in
	 *         the provided store that fall withing the given range.
	 */
	public List<KeyValueBean> keyRangeForStore(String storeName, String from, String to) {
		return rangeForKeyValueStore(storeName, store -> store.range(from, to));
	}

	/**
	 * Query a window store for key-value pairs representing the value for a
	 * provided key within a range of windows
	 * 
	 * @param storeName store to query
	 * @param key       key to look for
	 * @param from      time of earliest window to query
	 * @param to        time of latest window to query
	 * @return A List of {@link KeyValueBean}s representing the key-values for the
	 *         provided key across the provided window range.
	 */
	public List<KeyValueBean> windowedByKey(String storeName, String key, Long from, Long to) {

		// Lookup the WindowStore with the provided storeName
		final ReadOnlyWindowStore<String, Long> store = factoryBean.getKafkaStreams()
				.store(fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
		if (store == null) {
			throw new NotFoundException();
		}

		// fetch the window results for the given key and time range
		final WindowStoreIterator<Long> results = store.fetch(key, Instant.ofEpochMilli(from),
				Instant.ofEpochMilli(to));

		final List<KeyValueBean> windowResults = new ArrayList<>();
		while (results.hasNext()) {
			final KeyValue<Long, Long> next = results.next();
			// convert the result to have the window time and the key (for display purposes)
			windowResults.add(new KeyValueBean(key + "@" + next.key, next.value));
		}

		return windowResults;
	}

	/**
	 * Get the metadata for all of the instances of this Kafka Streams application
	 * 
	 * @return List of {@link HostStoreInfo}
	 */
	public List<HostStoreInfo> streamsMetadata() {
		return metadataService.streamsMetadata();
	}

	/**
	 * Get the metadata for all instances of this Kafka Streams application that
	 * currently has the provided store.
	 * 
	 * @param store The store to locate
	 * @return List of {@link HostStoreInfo}
	 */
	public List<HostStoreInfo> streamsMetadataForStore(String store) {
		return metadataService.streamsMetadataForStore(store);
	}

	/**
	 * Find the metadata for the instance of this Kafka Streams Application that has
	 * the given store and would have the given key if it exists.
	 * 
	 * @param store Store to find
	 * @param key   The key to find
	 * @return {@link HostStoreInfo}
	 */
	public HostStoreInfo streamsMetadataForStoreAndKey(String store, String key) {
		return metadataService.streamsMetadataForStoreAndKey(store, key, new StringSerializer());
	}

	/**
	 * Performs a range query on a KeyValue Store and converts the results into a
	 * List of {@link KeyValueBean}
	 * 
	 * @param storeName     The store to query
	 * @param rangeFunction The range query to run, i.e., all, from(start, end)
	 * @return List of {@link KeyValueBean}
	 */
	private List<KeyValueBean> rangeForKeyValueStore(String storeName,
			Function<ReadOnlyKeyValueStore<String, Long>, KeyValueIterator<String, Long>> rangeFunction) {

		// Get the KeyValue Store
		final ReadOnlyKeyValueStore<String, Long> store = factoryBean.getKafkaStreams()
				.store(fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
		
		final List<KeyValueBean> results = new ArrayList<>();
		
		// Apply the function, i.e., query the store
		final KeyValueIterator<String, Long> range = rangeFunction.apply(store);

		// Convert the results
		while (range.hasNext()) {
			final KeyValue<String, Long> next = range.next();
			results.add(new KeyValueBean(next.key, next.value));
		}

		return results;
	}

	private boolean thisHost(final HostStoreInfo host) {
		return host.getHost().equals(hostInfo.host()) && host.getPort() == hostInfo.port();
	}

}
