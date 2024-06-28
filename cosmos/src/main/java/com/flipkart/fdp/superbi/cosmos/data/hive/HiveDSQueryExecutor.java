package com.flipkart.fdp.superbi.cosmos.data.hive;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.jdbc.JdbcDSQueryExecutor;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import java.util.Map;

/**
 * Created by rajesh.kannan on 13/05/15.
 */
public class HiveDSQueryExecutor extends JdbcDSQueryExecutor {

	public HiveDSQueryExecutor(String name, String sourceType, String jdbcUrl, String username,
			String password,  Map<String, String> attributes) {
		super(name, sourceType, jdbcUrl, username, password, new HiveDSLConfig(attributes));
	}

	@Override public AbstractQueryBuilder getTranslator(DSQuery query,
			Map<String, String[]> paramValues) {
		return new HiveQueryBuilder(query,paramValues, (HiveDSLConfig)config, false);
	}

	public AbstractQueryBuilder getTranslator(DSQuery query,
			Map<String, String[]> paramValues, boolean executeVerticaQueryInHive) {
		return new HiveQueryBuilder(query,paramValues, (HiveDSLConfig)config, executeVerticaQueryInHive);
	}
	/*/
	 Caching has not been implemented yet
	 */
	@Override public QueryResult executeNative(Object object,
			ExecutionContext context,
			ICacheClient<String, QueryResult> cacheClient) {
		return executeNative(object, context);
	}
}
