package com.flipkart.fdp.superbi.cosmos;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.FStreamParserConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.hive.HiveDSLConfig;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum DataSourceType {
    VERTICA {
        @Override
        public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
            return new VerticaDSLConfig(dslConfig);
        }
    },
    ELASTIC_SEARCH {
        @Override
        public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
            return new ElasticParserConfig(dslConfig);
        }
    },
    FSTREAM {
        @Override
        public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
            return  new FStreamParserConfig(dslConfig);
        }
    },
    HDFS {
        @Override
        public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
            return new HiveDSLConfig(dslConfig);
        }
    },
    DRUID {
        @Override
        public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
            return new DruidDSLConfig(dslConfig);
        }
    },
    BIG_QUERY {
        @Override
        public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
            // DSLConfig must have BQ_USAGE_TYPE which should resolve to enum BQUsageType
            // Defualt is BQ_REALTIME
            return new BQDSLConfig(dslConfig);
        }
    };

    public abstract AbstractDSLConfig getDslConfig(Map<String, String> dslConfig);
}
