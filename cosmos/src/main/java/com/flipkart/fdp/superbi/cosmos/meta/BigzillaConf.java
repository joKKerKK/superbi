package com.flipkart.fdp.superbi.cosmos.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * User: aartika
 * Date: 6/9/14
 */
@Getter
@Setter
public class BigzillaConf {

    @JsonProperty
    public BigzillaHost bigzillaHost;

    @JsonProperty
    public int defaultRuleRollingWindowMins;

    @JsonProperty
    public int defaultRuleThreshold;

    @Getter
    @Setter
    public static class BigzillaHost {
        public String hostName;
        public int port;

        public BigzillaHost() {
        }

        public BigzillaHost(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
        }
    }

    public String selfHost;
    public int selfPort;
}
