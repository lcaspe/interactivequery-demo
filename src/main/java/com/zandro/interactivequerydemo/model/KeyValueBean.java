package com.zandro.interactivequerydemo.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 
 * A key-value bean.
 * 
 * @author Lizandro Caspe
 *
 */
@Data
@AllArgsConstructor
public class KeyValueBean {

	private String key;
	private Long value;

}
