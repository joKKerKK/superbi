package com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica;

import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.cache.CacheCleaner;
import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.jdbc.JdbcDSQueryExecutor;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by amruth.s on 31-10-2014.
 */
public final class VerticaDSQueryExecutor extends JdbcDSQueryExecutor {

	private static final Logger logger = LoggerFactory.getLogger(
			CacheCleaner.class);
	private static final String cacheLoggerformatString = "[%s] CACHE class=%s msg=%s"; //class_name  msg


	public VerticaDSQueryExecutor(String name, String sourceType, String jdbcUrl, Properties properties, Map<String, String> attributes) {
		super(name, sourceType, jdbcUrl, properties, new VerticaDSLConfig(attributes));
	}

	@LogExecTime
	@Override public  final AbstractQueryBuilder getTranslator(DSQuery query,
			Map<String, String[]> paramValues) {
		return new VerticaQueryBuilder(query, paramValues, (VerticaDSLConfig) config);
	}

	@LogExecTime
	@Override
	public QueryResult executeNative(Object object, ExecutionContext context,ICacheClient<String,QueryResult> cacheClient){
		Optional<QueryResult> queryResultOp=Optional.empty();
		QueryResult queryResult;

		Timer getTime=new Timer();
		getTime.start();

		try {
			queryResultOp = cacheClient.get(String.valueOf(object));
		}catch(Exception e){
			e.printStackTrace();
			logger.error(String.format(cacheLoggerformatString, new Date(), this.getClass().getName(), "Exception while calling get on cache client with key="+String.valueOf(object)));
			queryResult= dao.execute(String.valueOf(object));
		}
		if(queryResultOp.isPresent()){
			getTime.stop();
			logger.error(String.format(cacheLoggerformatString, new Date(), this.getClass().getName(), "WOW  [HIT] time taken="+getTime.getTimeTakenMs()+"ms for query  "+String.valueOf(object)));
			return queryResultOp.get();
		}
		getTime.stop();
		logger.error(String.format(cacheLoggerformatString, new Date(), this.getClass().getName(), "OHH  [MISS] time taken="+getTime.getTimeTakenMs()+"ms for query  "+String.valueOf(object)));
		queryResult= dao.execute(String.valueOf(object));
		return queryResult;
	}
}
