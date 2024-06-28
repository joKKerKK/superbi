package com.flipkart.fdp.superbi.web.configurations;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DataSource implements Serializable{
    private String storeIdentifier;
    private String sourceType;
    private Map<String, String> attributes;
    private CircuitBreakerProperties circuitBreakerProperties;
    private Map<String,String> dslConfig = new HashMap<>();
    private Integer limit = 100000;
}
