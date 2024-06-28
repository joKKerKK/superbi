package com.flipkart.fdp.superbi.cosmos.data.hive;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos.TableCatalogInfo;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.sql.SQLQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Table;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.google.common.base.Optional;

import java.text.MessageFormat;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.asynchttpclient.AsyncHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.flipkart.fdp.superbi.cosmos.utils.Constants.HIVE;
import static com.flipkart.fdp.superbi.cosmos.utils.Constants.TABLE;

/**
 * Created by rajesh.kannan on 13/05/15.
 */
public class HiveQueryBuilder extends SQLQueryBuilder {

	private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	private static final Logger logger = LoggerFactory.getLogger(
			HiveQueryBuilder.class);
	private Fact fact;
	private BadgerClient badgerClient;
	private boolean isBadgerEnabled;

	boolean executeVerticaQueryInHive;
	static {
		try {
			// since the driver is not jdbc4 compliant, loaded manually.
			Class.forName(HIVE_DRIVER);
		} catch (ClassNotFoundException e) {
			logger.error("Driver class "+HIVE_DRIVER + " not present in the path", e);
		}
	}

	HiveDSLConfig dslConfig = (HiveDSLConfig) config;
	public HiveQueryBuilder(DSQuery query, Map<String, String[]> paramValues,
					AbstractDSLConfig config, boolean executeVerticaQueryInHive, BadgerClient badgerClient) {
		super(query, paramValues, config);
		this.executeVerticaQueryInHive = executeVerticaQueryInHive;
		this.badgerClient = badgerClient;
		this.isBadgerEnabled =  ((HiveDSLConfig) config).getIsBadgeEnabled();
	}

	public HiveQueryBuilder(DSQuery query, Map<String, String[]> paramValues,
							AbstractDSLConfig config, boolean executeVerticaQueryInHive) {
		this(query, paramValues, config, executeVerticaQueryInHive, BadgerClient.getInstance());
	}

	private Fact getFact(String factName) {
		if(fact == null)
			fact = MetaAccessor.get().getFactByName(factName);
		return fact;
	}

	@Override public String getHistogram(String columnName, long interval) {
		return "(round(" + columnName +  "/" + interval + ")" + "*" + interval + ")";
	}

	@Override public String getDateHistogram(String dateColumn,
			Optional<String> timeColumn, long interval) {
		if(timeColumn.isPresent())
			return "unix_timestamp( cast( (" + dslConfig.getDateExpression(dateColumn, timeColumn.get()) +") as string), '"+dslConfig.getDateTimeSurrugatePattern()+"')";
		return "unix_timestamp( cast(" + dateColumn + " as string), '"+dslConfig.getDateSurrugatePattern()+"')";
	}

	@Override public String getFractileExpr(String columnName, Double fractileVal) {
		return "percentile_approx("+columnName + ", "+fractileVal +")";
	}

	@Override public String getNativeTableByFactName(String factName) {
		Fact f= getFact(factName);
		return getHiveTable(f.getOrg(), f.getNamespace(), f.getName());
	}

	private String getHiveTable(String org, String namespace, String factName)
	{
		// Config driven changes for badger information
		// Assuming badger is the source of truth
		if(isBadgerEnabled){
			try{
				TableCatalogInfo tableCatalogInfo = badgerClient.getTableCatalogInfo(factName, HIVE, TABLE).get(0);
				return String.format("%s.%s",tableCatalogInfo.getDatabaseName(), tableCatalogInfo.getTableName());
			}catch(Exception e){
				logger.error("Could not get table catalog info for \n"+factName, e);
				e.printStackTrace();
				throw new ServerSideException(
						MessageFormat.format("Could not get table catalog info for {0} due to {1} ", factName,
								e.getMessage()));
			}
		}
		else{
			return String.format("bigfoot_external_neo.%s_%s__%s", org, namespace, factName);
		}
	}
	@Override public String quoteAlias(String s) {
		return "`"+s+"`";
	}

	@Override public String getPhysicalDimensionTableByName(String dimensionName) {
		switch(dimensionName)
		{
			case "date_dim":
			case "date_dim_cal":
				return "bigfoot_common.date_dim_cal";
			case "time_dim":
				return "bigfoot_common.time_dim";
			default:
				return getNativeTableByCosmosTableName(super.getPhysicalDimensionTableByName(dimensionName));
		}

	}

	@Override public String getCosmosTableNameByFactName(String factName) {
		Fact f= getFact(factName);
		return String.format("b_%s_%s.%s", f.getOrg(), f.getNamespace(), factName);
	}

	@Override public String overwriteFactOrDimensionNameIfNeeded(
			String factOrDimension) {
		if(executeVerticaQueryInHive)
			return factOrDimension.replace("vertica", "hive");
		return factOrDimension;
	}

	@Override public String getNativeTableByCosmosTableName(String tableName) {

		if(tableName.matches("b_\\w+_\\w+\\.\\w+")) //b_org_namespace_factname
		{
			Table table = MetaAccessor.get().getTableByName(tableName);
			return getHiveTable(table.getOrg(), table.getNamespace(), table.getName().split("\\.")[1]);

//			String tokens[] = tableName.split("[._]", 4);
//			return getHiveTable(tokens[1], tokens[2], tokens[3]);
		}
		return tableName;
	}


}
