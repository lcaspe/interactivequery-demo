package com.zandro.interactivequerydemo.model;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class HostStoreInfo {

	private String host;
	private int port;
	private Set<String> storeNames;

}
