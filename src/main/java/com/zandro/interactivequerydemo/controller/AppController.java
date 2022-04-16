package com.zandro.interactivequerydemo.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.zandro.interactivequerydemo.component.Producer;
import com.zandro.interactivequerydemo.model.HostStoreInfo;
import com.zandro.interactivequerydemo.model.KeyValueBean;
import com.zandro.interactivequerydemo.service.QueryService;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class AppController {

	private final Producer producer;
	private final QueryService queryService;
	private static final Logger LOGGER = LoggerFactory.getLogger(AppController.class);

	@PostMapping("/publish")
	public void publish(@RequestParam("key") int key, @RequestParam("value") String value) {
		producer.publish(key, value);
	}
	
	@PostMapping("/publish-testTopic")
	public void publishToTestTopic(@RequestParam("key") int key, @RequestParam("value") String value) {
		producer.publishForTestTopic(key, value);
	}

	@GetMapping("/keyvalue/{storeName}/{key}")
	public KeyValueBean getFromStoreByKey(@PathVariable String storeName, @PathVariable String key) {
		LOGGER.info("Getting KeyValueBean from \"{}\" with \"{}\" key...", storeName, key);
		return queryService.byKey(storeName, key);
	}

	@GetMapping("/keyvalues/{storeName}/all")
	public List<KeyValueBean> getAllFromStore(@PathVariable String storeName) {
		LOGGER.info("Getting KeyValueBean list from \"{}\"...", storeName);
		return queryService.allForStore(storeName);
	}

	@GetMapping("/keyvalues/{storeName}/range/{from}/{to}")
	public List<KeyValueBean> getFromStoreWithinRange(@PathVariable String storeName, @PathVariable String from,
			@PathVariable String to) {
		LOGGER.info("Getting KeyValueBean list from \"{}\" with range from \"{}\" to \"{}\"...", storeName, from, to);
		return queryService.keyRangeForStore(storeName, from, to);
	}

	@GetMapping("/windowed/{storeName}/{key}/{from}/{to}")
	public List<KeyValueBean> getFromStoreWithinRange(@PathVariable String storeName, @PathVariable String key,
			@PathVariable long from, @PathVariable long to) {
		LOGGER.info(
				"Getting KeyValueBean list from windowed \"{}\" with \"{}\" key and with range from \"{}\" to \"{}\"...",
				storeName, key, from, to);
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
	public HostStoreInfo getStreamsMetadataByStoreAndKey(@PathVariable String storeName, @PathVariable String key) {
		LOGGER.info("Getting instance metadata that has the \"{}\" store with \"{}\" key...", storeName, key);
		return queryService.streamsMetadataForStoreAndKey(storeName, key);
	}

}
