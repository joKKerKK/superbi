package com.flipkart.fdp.superbi.brv2.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DataSource implements Serializable{
    private String storeIdentifier;
    private String sourceType;
    private Map<String, Object> attributes;
    private CircuitBreakerProperties circuitBreakerProperties;
    private Map<String,String> dslConfig = new HashMap<>();
}
