package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import static com.flipkart.fdp.superbi.cosmos.hystrix.ServiceConfigDefaults.inject;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticSearchDSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.FStreamDSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.mysql.MysqlExecutor;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.hive.HiveDSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.hystrix.ServiceConfigDefaults;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Source;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Properties;

/**
 * Created by amruth.s on 25/08/14.
 */
public enum ExecutorType {
    ELASTIC_SEARCH {
        @Override
        public DSQueryExecutor initializeExecutorFor(final Source source) {
            final Map<String, String> attributes = source.getAttributes();
            final String host = attributes.get("host");
            int port = Integer.valueOf(attributes.get("port"));
            final String clusterName = attributes.get("clusterName");
            Preconditions.checkNotNull(host);
            Preconditions.checkNotNull(port);
            return new ElasticSearchDSQueryExecutor(source.getName(), source.getSourceType(), host, port,
                    Optional.fromNullable(clusterName), attributes);
        }
    },
    VERTICA {
        @Override
        public DSQueryExecutor initializeExecutorFor(final Source source) {
            final Map<String, String> attributes = source.getAttributes();
            String jdbcUrl = attributes.get("jdbcUrl");
            String username = attributes.get("username");
            String password = attributes.get("password");
            Preconditions.checkNotNull(jdbcUrl);
            Preconditions.checkNotNull(username);
            Preconditions.checkNotNull(password);

            Properties properties = new Properties();
            properties.put(VerticaDSLConfig.VERTICA_JDBC_USERNAME, username);
            properties.put(VerticaDSLConfig.VERTICA_JDBC_PASSWORD, password);
            if (attributes.containsKey(VerticaDSLConfig.VERTICA_LOAD_BALANCE))
                properties.put(VerticaDSLConfig.VERTICA_LOAD_BALANCE, attributes.get(VerticaDSLConfig.VERTICA_LOAD_BALANCE));
            if (attributes.containsKey(VerticaDSLConfig.BACKUP_SERVER_NODE))
                properties.put(VerticaDSLConfig.BACKUP_SERVER_NODE, attributes.get(VerticaDSLConfig.BACKUP_SERVER_NODE));
            return new VerticaDSQueryExecutor(source.getName(), source.getSourceType(), jdbcUrl, properties, attributes);
        }
    },
    MYSQL {
        @Override public DSQueryExecutor initializeExecutorFor(Source source) {
            final Map<String, String> attributes = source.getAttributes();
            String jdbcUrl = attributes.get("jdbcUrl");
            String username = attributes.get("username");
            String password = attributes.get("password");
            Preconditions.checkNotNull(jdbcUrl);
            Preconditions.checkNotNull(username);
            Preconditions.checkNotNull(password);
            return new MysqlExecutor(source.getName(), source.getSourceType(), jdbcUrl, username,
                    password, attributes);
        }
    },
    HDFS {
        @Override
        public DSQueryExecutor initializeExecutorFor(final Source source) {
            final Map<String, String> attributes = source.getAttributes();
            String jdbcUrl = attributes.get("jdbcUrl");
            String username = attributes.get("username");
            String password = attributes.get("password");
            Preconditions.checkNotNull(jdbcUrl);
            Preconditions.checkNotNull(username);
            Preconditions.checkNotNull(password);
            return new HiveDSQueryExecutor(source.getName(), source.getSourceType(), jdbcUrl, username, password, attributes);
        }
    },
    FSTREAM {
        @Override
        public DSQueryExecutor initializeExecutorFor(final Source source) {
            final Map<String, String> attributes = source.getAttributes();
            String host = attributes.get("host");
            String port = attributes.get("port");
            Preconditions.checkNotNull(host);
            Preconditions.checkNotNull(port);
            return new FStreamDSQueryExecutor(source.getName(), source.getSourceType(), host, port, attributes);
        }
    };

    public abstract DSQueryExecutor initializeExecutorFor(final Source source);

    public static DSQueryExecutor initializeExecutor(Source source) {
        String maxThreads = source.getAttributes().get("max_threads");
        if(maxThreads != null){
            inject(new ServiceConfigDefaults.Config.Builder().withMaxThreads(Integer.valueOf(maxThreads)).forServiceGroup(source.getName()));
        }
        return ExecutorType.valueOf(source.getSourceType()).initializeExecutorFor(source);
    }
}
