package com.zandro.interactivequerydemo.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.zandro.interactivequerydemo.engine.Producer;
import com.zandro.interactivequerydemo.model.HostStoreInfo;
import com.zandro.interactivequerydemo.model.KeyValueBean;
import com.zandro.interactivequerydemo.service.QueryService;
import com.zandro.interactivequerydemo.service.RestService;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class AppController {

	private final RestService restService;
	private final Producer producer;
	private final QueryService queryService;
	private static final Logger LOGGER = LoggerFactory.getLogger(AppController.class);

	@PostMapping("/publish")
	public void publish(@RequestParam("key") int key, @RequestParam("value") String value) {
		producer.publish(key, value);
	}

	@GetMapping("/count/{word}")
	public Long getCount(@PathVariable String word) {
		LOGGER.info(String.format("Getting word \"%s\" count...", word));
		return restService.getCount(word);
	}

	@GetMapping("/keyvalue/{storeName}/{key}")
	public KeyValueBean getFromStoreByKey(@PathVariable String storeName, @PathVariable String key) {
		LOGGER.info(String.format("Getting KeyValueBean from \"%s\" with \"%s\" key...", storeName, key));
		return queryService.byKey(storeName, key);
	}

	@GetMapping("/keyvalues/{storeName}/all")
	public List<KeyValueBean> getAllFromStore(@PathVariable String storeName) {
		LOGGER.info(String.format("Getting KeyValueBean list from \"%s\"...", storeName));
		return queryService.allForStore(storeName);
	}

	@GetMapping("/keyvalues/{storeName}/range/{from}/{to}")
	public List<KeyValueBean> getFromStoreWithinRange(@PathVariable String storeName, @PathVariable String from,
			@PathVariable String to) {
		LOGGER.info(String.format("Getting KeyValueBean list from \"%s\" with range from \"%s\" to \"%s\"...",
				storeName, from, to));
		return queryService.keyRangeForStore(storeName, from, to);
	}

	@GetMapping("/windowed/{storeName}/{key}/{from}/{to}")
	public List<KeyValueBean> getFromStoreWithinRange(@PathVariable String storeName, @PathVariable String key,
			@PathVariable long from, @PathVariable long to) {
		LOGGER.info(String.format(
				"Getting KeyValueBean list from windowed \"%s\" with \"%s\" key and with range from \"%d\" to \"%d\"...",
				storeName, key, from, to));
		return queryService.windowedByKey(storeName, key, from, to);
	}
	
	@GetMapping("/instances")
	public List<HostStoreInfo> getAllInstancesMetadata() {
		LOGGER.info("Getting all instances' metadata...");
		return queryService.streamsMetadata();
	}
	
	@GetMapping("/instances/{storeName}")
	public List<HostStoreInfo> getAllInstancesMetadata(@PathVariable String storeName) {
		LOGGER.info("Getting all instances' metadata that currently has the provided store...");
		return queryService.streamsMetadataForStore(storeName);
	}
	
	@GetMapping("/instance/{storeName}/{key}")
	public HostStoreInfo getStreamsMetadataByStoreAndKey(@PathVariable String storeName,
			@PathVariable String key) {
		LOGGER.info(String.format("Getting instance metadata that has the \"%s\" store with \"%s\" key...", storeName,
				key));
		return queryService.streamsMetadataForStoreAndKey(storeName, key);
	}

}
