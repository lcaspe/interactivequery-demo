package com.zandro.interactivequerydemo.model;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 
 * Object that contains host store info.
 * 
 * @author Lizandro Caspe
 *
 */
@Data
@AllArgsConstructor
public class HostStoreInfo {

	private String host;
	private int port;
	private Set<String> storeNames;

}
