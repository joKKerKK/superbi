package com.flipkart.fdp.superbi.cosmos.hystrix;

import com.google.common.base.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by amruth.s on 06/01/15.
 *
 * Static class for configuring services
 * Should be used when the services are getting initialized
 */
public class ServiceConfigDefaults {

    private static final int DEFAULT_MAX_THREADS = 10;

    public static class Config {

        public static class Builder {
            private Config config = new Config();

            public Config forServiceGroup(String serviceGroupName) {
                config.serviceGroupName = serviceGroupName;
                return config;
            }

            public Builder withMaxThreads(int maxThreads) {
                config.maxThreads = maxThreads;
                return this;
            }
            public Builder withRejectionLimit(int rejectionLimit) {
                config.rejectionLimit = Optional.of(rejectionLimit);
                return this;
            }
            public Builder withMaxQueueSize(int maxQueueSize) {
                config.maxQueueSize = Optional.of(maxQueueSize);
                return this;
            }
        }

        private String serviceGroupName;
        private int maxThreads;
        private Optional<Integer> maxQueueSize = Optional.absent();
        private Optional<Integer> rejectionLimit = Optional.absent();

        private Config(){}

        public String getServiceGroupName() {
            return serviceGroupName;
        }

        public int getMaxThreads() {
            return maxThreads;
        }

        public Optional<Integer> getRejectionLimit()
        {
            return rejectionLimit;
        }

        public Optional<Integer> getMaxQueueSize()
        {
            return maxQueueSize;
        }
    }


    private static ConcurrentHashMap<String, Config> configurations = new ConcurrentHashMap<String, Config>();

    public static Config getByGroupName (String groupName) {
        Optional<Config> configOptional = Optional.fromNullable(configurations.get(groupName));
        return configOptional.isPresent()?
                    configOptional.get():
                    inject(new Config.Builder().withMaxThreads(DEFAULT_MAX_THREADS).forServiceGroup(groupName));
    }

    private static class ConfigAlreadyInjected extends RuntimeException {

    }

    public static Config inject( Config config) throws ConfigAlreadyInjected {
        final String serviceGroupName = config.getServiceGroupName();
        if(!configurations.containsKey(serviceGroupName)) {
            configurations.put(serviceGroupName, config);
        }
        return configurations.get(serviceGroupName);
    }

}
