package com.flipkart.fdp.superbi.cosmos.data.api.execution.sql;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.*;
import com.flipkart.fdp.superbi.dsl.query.exp.*;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.DefaultCriteriaVisitor;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

/**
 * Created by rajesh.kannan on 13/05/15.
 */
public abstract class SQLQueryBuilder extends AbstractQueryBuilder {

	private static org.slf4j.Logger logger = LoggerFactory.getLogger(SQLQueryBuilder.class);

	private List<String> selectExpressions = Lists.newArrayList();



	private String from;
	private String factName;
	private Optional<String> tableAlias = Optional.absent();

	private final List<String> rootCriteriaString = Lists.newArrayList();
	private List<String> histogramExpressions = Lists.newArrayList();
	private List<String> histogramExpressionAliases = Lists.newArrayList();
	private List<String> groupByExpressions = Lists.newArrayList();
	private List<String> orderByExpressions = Lists.newArrayList();
	private List<String> joinExpressions = Lists.newArrayList();

	SQLDSLConfig castedConfig;
	private Optional<Integer> limit = Optional.of(0);

	public SQLQueryBuilder(DSQuery query, Map<String, String[]> paramValues,
			AbstractDSLConfig config) {
		super(query, paramValues, config);
		castedConfig = (SQLDSLConfig) config;
	}

	public abstract String getHistogram(String columnName, long interval);

	public abstract String getDateHistogram(String dateColumn, Optional<String> timeColumn, long interval);
	public abstract String getFractileExpr(String columnName, Double fractileVal);

	public abstract String overwriteFactOrDimensionNameIfNeeded(
			String factOrDimension);



	@Override public void visit(SelectColumn.SimpleColumn column) {
		selectExpressions.add(
				getModifiedColumnName(column.colName) + " as " + quoteAlias(
						column.getAlias()));
	}

	public String getPhysicalDimensionTableByName(String dimensionName)
	{
		return MetaAccessor.get()
					.getDimensionByName(dimensionName).getTableName();

	}

	protected String getModifiedColumnName(String dimColExpr) {
		String[] parts = dimColExpr.split("\\.");
		if (parts.length == 4) {
			final String factForeignKey = parts[0];
			final String dimensionName = parts[1];
			final String dimensionTableName = getPhysicalDimensionTableByName(
					dimensionName);
			final String dimPk = dimensionName + "_key";
			final String dimensionTableAlias = parts[0] + "_" + parts[1];
			final String joinExpression =
					" left outer join " + dimensionTableName + " as " + dimensionTableAlias + " on "
							+ dimensionTableAlias + "." + dimPk + " = " + String.format("`%s`",factName) + "." + factForeignKey;
			if (!joinExpressions.contains(joinExpression))
				joinExpressions.add(joinExpression);
			return dimensionTableAlias + "." + parts[3];
		}
		return dimColExpr;
	}

	@Override public void visit(SelectColumn.Aggregation column,
			SelectColumn.AggregationOptions options) {

		String expr;
		if(column.aggregationType == AggregationType.FRACTILE) {
			expr = getFractileExpr(getModifiedColumnName(column.colName), options.fractile.get()) + " as " + quoteAlias(column);;
		}
		else
		{
			expr = SQLDSLConfig.getAggregationString(column.aggregationType) + "(" + (column.aggregationType.equals(
					AggregationType.DISTINCT_COUNT) ? "distinct " : "") +
					getModifiedColumnName(column.colName) + ") as " + quoteAlias(column);
		}
		selectExpressions.add(expr);


	}

	@Override public void visit(SelectColumn.ConditionalAggregation column) {
		final RootCriteriaBuilder caseCriteriaBuilder = new RootCriteriaBuilder();
		column.criteria.accept(caseCriteriaBuilder);
		final String caseExpression =
				" CASE WHEN " + caseCriteriaBuilder.criteriaBuilder.toString() + " THEN " + getModifiedColumnName(column.colName)
						+ " END";
		selectExpressions.add(SQLDSLConfig.getAggregationString(column.type) + "("
				+ (column.type.equals(AggregationType.DISTINCT_COUNT) ? "distinct " :"")+
				caseExpression + ") as " + quoteAlias(column));

	}

	@Override public void visitFrom(String fromTable) {
		String[] partsFrom = fromTable.split("\\.");
		this.from = partsFrom.length == 1 ?
				getNativeTableByFactName(fromTable):
				getNativeTableByCosmosTableName(fromTable);
		this.factName = partsFrom[partsFrom.length-1];
	}
	public abstract String getNativeTableByFactName(String factName);
	public String getCosmosTableNameByFactName(String factName)
	{
		return getNativeTableByFactName(factName);
	}
	public String getNativeTableByCosmosTableName(String tableName)
	{
		return tableName;
	}

	@Override public void visit(DateRangePredicate dateRangePredicate) {
		final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
		dateRangePredicate.accept(filterBuilder);
		final String criteriaString = filterBuilder.criteriaBuilder.toString();
		if (!criteriaString.equals(""))
			rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
	}

	@Override public void visit(Criteria criteria) {
		final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
		criteria.accept(filterBuilder);
		final String criteriaString = filterBuilder.criteriaBuilder.toString();
		if (!criteriaString.equals(""))
			rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
	}

	@Override public void visitGroupBy(String groupByColumn) {
		groupByExpressions.add(getModifiedColumnName(groupByColumn));
	}

	@Override public void visitHistogram(String alias, String columnName, long from, long to, long interval) {

		String quotedAlias = quoteAlias(alias);
		String bucketExpression = getHistogram(columnName, interval);
		String histogramExpr = " " + bucketExpression +  " as " + quotedAlias;
		selectExpressions.add(histogramExpr);
		// Alias of the bucket expression does not work in where clause for some reason - figure out a way
		rootCriteriaString.add(bucketExpression + ">=" + from);
		rootCriteriaString.add(bucketExpression + "<" + to);


		histogramExpressionAliases.add(quotedAlias);
		histogramExpressions.add(bucketExpression);

	}

	@Override public void visitDateHistogram(String alias, String columnName,
																					 Date from, Date to, long intervalMs,
																					 SelectColumn.DownSampleUnit downSampleUnit) {

//		visitDateRange(columnName, from, to);

		String quotedAlias = quoteAlias(alias);
		final Optional<String> timeColumnOptional = castedConfig.getMatchingTimeColumnIfPresent(
				columnName, getCosmosTableNameByFactName(this.factName));
		final String bucketExpression = castedConfig.isDateColumnDegenerateDimension(columnName, factName) ?
				"unix_timestamp(" + columnName + ")" : getDateHistogram(columnName, timeColumnOptional, intervalMs);
		/**
		 * Explanation of logic below
		 *
		 * Here we have converted input date to unix_timestamp which is independent of any timezone i.e 1970 Jan 1 00:00 GMT is `0` in unix timestamp.
		 * Now we need to convert timestamp in buckets of intervalMs , but if we just do it by divinding value by intervalMs then we will get those buckets in GMT zone and when it will be converted back
		 * to IST zone 5:30 (19800 sec) will be added and thus we won't get required buckets. So to adjust that value we are adding 19800 before creating buckets and will subtract same after getting buckets
		 * and thus getting desired buckets.
		 * Ex : 1 July 2019 10:02 IST will be passed as 1 July 2019 2019 3:32 IST (due to addditon)
		 * When creating hourly buckets we will get 1 July 2019 10:00 GMT
		 * Again we will subtract 19800 so we will get 1 July 2019 03:30 GMT
		 * Now server will add 5:30 during FROM_UNIXTIME and we will get 1 July 2019 10:00 i.e our desired result
		 *
		 */
		long intervalSeconds = intervalMs / 1000;
		final Long GMT_TIMEZONE_OFFSET = 19800L;
		final String bucketExpressionTimezoneAdjusted = bucketExpression + " + " + GMT_TIMEZONE_OFFSET;
		final String bucketExpressionFinal = "(" + bucketExpressionTimezoneAdjusted + ")/" + intervalSeconds;
		String bucketExpressionWithInterval = "";
		if(castedConfig.isDateColumnDegenerateDimension(columnName, factName)) {
			/**
			 * Below will output something like
			 * FROM_UNIXTIME(cast(FLOOR((unix_timestamp(forward_unit_live_hbase_snapshot_fact.unit_creation_timestamp) + 19800)/3600) as bigint)*3600 -19800)
			 */
			bucketExpressionWithInterval = "FROM_UNIXTIME(" +
					"(" +
					"cast(FLOOR(" + bucketExpressionFinal + ") as bigint) * " + intervalSeconds +
					") - " + GMT_TIMEZONE_OFFSET
					+ ")";
		}
		else {
			bucketExpressionWithInterval = "round(" + bucketExpression + "/"+ intervalSeconds + ")";
		}

		String selectHistogramExpr = bucketExpressionWithInterval
				+ " as " + quotedAlias + " ";

		selectExpressions.add(selectHistogramExpr);
		// Alias of the bucket expression does not work in where clause for some reason - figure out a way
		rootCriteriaString.add(bucketExpression + ">=" + from.getTime()/1000);
		rootCriteriaString.add(bucketExpression + "<" + to.getTime()/1000);
		histogramExpressionAliases.add(quotedAlias);
		histogramExpressions.add(bucketExpressionWithInterval);
	}

	@Override public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
		orderByExpressions.add(
				quoteAlias(orderByColumn) + ' ' + castedConfig.getOrderByType(type));
	}

	@Override public void visit(Optional<Integer> limit) {
		this.limit = limit;
	}

	@Override public void visitDateRange(String column, Date start, Date end) {
		final StringBuilder dateRangeBuilder = new StringBuilder();
		if(castedConfig.isDateColumnDegenerateDimension(column, factName)){
			String dateExpression = "unix_timestamp(" + column + ")";
			dateRangeBuilder.append(dateExpression);
			dateRangeBuilder.append(">=").append(start.getTime()/1000);
			if (end != null) {
				dateRangeBuilder.append(" and ")
						.append(dateExpression)
						.append("<")
						.append(end.getTime()/1000);
			}		}
		else{
			Optional<String> timeColumnOptional = castedConfig.getMatchingTimeColumnIfPresent(
					column, getCosmosTableNameByFactName(factName));
			if (timeColumnOptional.isPresent()) {
				String timeColumn = timeColumnOptional.get();
				String dateTimeExpression = castedConfig.getDateExpression(column,
						timeColumn);
				String dateTimeSurrugatePattern = castedConfig.getDateTimeSurrugatePattern();
				DateTimeFormatter dtf = DateTimeFormat.forPattern(
						dateTimeSurrugatePattern);
				String startValue = dtf.print(start.getTime());
				dateRangeBuilder.append(dateTimeExpression)
						.append(">=")
						.append(startValue);
				if (end != null) {
					String endValue = dtf.print(end.getTime());
					dateRangeBuilder.append(" and ")
							.append(dateTimeExpression)
							.append("<")
							.append(endValue);
				}
			} else {
				DateTimeFormatter dtf = DateTimeFormat.forPattern(castedConfig.getDateSurrugatePattern());
				String startValue = dtf.print(start.getTime());
				dateRangeBuilder.append(column);
				dateRangeBuilder.append(">=").append(startValue);
				if (end != null) {
					String endValue = dtf.print(end.getTime());
					dateRangeBuilder.append(" and ")
							.append(column)
							.append("<")
							.append(endValue);
				}
			}
		}
		rootCriteriaString.add(dateRangeBuilder.toString());
	}

	@Override
	protected Object buildQueryImpl() {
		final StringBuilder queryBuffer = new StringBuilder();

		queryBuffer.append("select ");
		Joiner.on(",").appendTo(queryBuffer, selectExpressions);

		String fromWithQuotes = String.format("`%s`",from);
		queryBuffer.append(" from ").append(fromWithQuotes);

		if(!factName.equals(from)){
			String factNameWithQuotes = String.format("`%s`", factName);
			queryBuffer.append(" ").append(factNameWithQuotes);
		}


		if(!joinExpressions.isEmpty()) {
			queryBuffer.append(" ");
			Joiner.on(" ").appendTo(queryBuffer, joinExpressions);
		}

		if(!rootCriteriaString.isEmpty()) {
			queryBuffer.append(" where ");
			Joiner.on(" AND ").appendTo(queryBuffer, rootCriteriaString);
		}

		List<String> allGroupByExprs = getAllGroupbyExpressions();
		if(!allGroupByExprs.isEmpty()) {
			queryBuffer.append(" group by ");
			Joiner.on(",").appendTo(queryBuffer, allGroupByExprs);
		}

		List<String> allOrderByAliases =  getAllOrderbyExpressions();
		if(!allOrderByAliases.isEmpty()) {
			queryBuffer.append(" order by ");
			Joiner.on(",").appendTo(queryBuffer, allOrderByAliases);
		}

		if (limit.isPresent()) {
			queryBuffer.append(" limit ").append(limit.get());
		}

		System.out.println(queryBuffer.toString());
		return queryBuffer.toString();
	}

	private List<String> getAllOrderbyExpressions() {
		List<String> list = Lists.newArrayList();
		list.addAll(histogramExpressionAliases);
		list.addAll(orderByExpressions);
		return list;
	}

	private List<String> getAllGroupbyExpressions() {
		List<String> list = Lists.newArrayList();
		list.addAll(histogramExpressions);
		list.addAll(groupByExpressions);
		return list;
	}

	private static String commaJoin(List<String> list) {
		return Joiner.on(",").join(list);
	}

	private static String commaJoin(List<String> list,
			java.util.function.Function<String, String> converter) {
		return list.stream().map(converter).collect(Collectors.joining(","));
	}

	private static String buildJoinConditions(List<String> cols, String alias1,
			String alias2) {
		return cols.stream()
				   .map(col -> alias1 + "." + col + " = " + alias2 + "." + col)
				   .collect(Collectors.joining(" AND "));
	}


	public abstract String quoteAlias(String s);
	public  String quoteAlias(SelectColumn col)
	{
		return quoteAlias(col.getAlias());
	}

	class PredicateNodeBuilder extends DefaultCriteriaVisitor
			implements CriteriaVisitor {
		private final Predicate predicate;
		private String columnName;
		private List<Object> values = Lists.newArrayList();
		private boolean isParamValueMissing = false;

		public PredicateNodeBuilder(Predicate predicate) {
			this.predicate = predicate;
		}

		@Override public CriteriaVisitor visit(Exp expression) {

			if (expression instanceof CompositeColumnExp && columnName == null) {

				columnName = ((CompositeColumnExp) expression).convertToProperExpression(
						s -> getModifiedColumnName(s));

			} else if (expression instanceof ColumnExp && columnName == null) {
				columnName = getModifiedColumnName(
						((ColumnExp) expression).evaluateAndGetColName(
								paramValues));
			}
			return this;
		}

		@Override public CriteriaVisitor visit(EvalExp expression) {
			if (expression instanceof LiteralEvalExp) {
				values.add(((LiteralEvalExp) expression).value);
			}
			return this;
		}

		@Override public CriteriaVisitor visit(Param param) {
			try {
				final Object value = param.getValue(paramValues);
				if (param.isMultiple)
					values.addAll((Collection<?>) value);
				else {
					values.add(value);
				}
			} catch (Exception e) {
				logger.warn(String.format("Filter %s, is ignored in the query since the value is missing. ", param.name)+ e.getMessage());
				isParamValueMissing = true;
			}
			return this;

		}

		public String getNode() {
			if (isParamValueMissing) {
				return "";
			}
			//            final ObjectNode node = jsonNodeFactory.objectNode();
			final StringBuilder predicateBuilder = new StringBuilder();
			if(predicate.getType(paramValues) != Predicate.Type.native_filter)
				predicateBuilder.append(columnName)
							.append(" ")
							.append(SQLDSLConfig.getPredicateStringFor(
									predicate.getType(paramValues)));
			switch (predicate.getType(paramValues)) {
			case not_in:
			case in: {
				if (values.isEmpty())
					throw new RuntimeException(
							"At-least one value is expected for if in parameter is passed");
				predicateBuilder.append("(");
				Iterable<Object> wrappedObjects = Iterables.transform(values,
						new Function<Object, Object>() {
							@Nullable @Override public Object apply(Object input) {
								return SQLDSLConfig.getWrappedObject(input);
							}
						});
				Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
				predicateBuilder.append(")");
				break;
			}
			case native_filter:
				predicateBuilder.append(values.get(0));
				break;
			case eq:
			case neq:
			case like:
			case lt:
			case lte:
			case gt:
			case gte: {
				predicateBuilder.append(
						SQLDSLConfig.getWrappedObject(values.get(0)));
				break;
			}
			case is_null:
			case is_not_null:
				// no need to add values to query for nulls
				break;
			default:
				throw new UnsupportedOperationException(
						"There are no handlers for the predicate " + predicate.getType(paramValues));
			}
			return predicateBuilder.toString();
		}
	}

	class RootCriteriaBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor {

		private final StringBuilder criteriaBuilder = new StringBuilder();

		@Override public CriteriaVisitor visit(Predicate predicate) {
			final PredicateNodeBuilder localBuilder = new PredicateNodeBuilder(
					predicate);
			predicate.accept(localBuilder);
			String predicateString = localBuilder.getNode();
			if (!(predicateString.equals(""))) {
				criteriaBuilder.append(predicateString);
			}
			return new DefaultCriteriaVisitor();
		}

		@Override public CriteriaVisitor visit(LogicalOp logicalOp) {

			criteriaBuilder.append("(");
			final List<String> criteriaNodes = Lists.newArrayList();
			criteriaNodes.add(" 1=1 "); // A dummy criteria if none of the criteria are valid (coz of invalid params)
			for (Criteria criteria : logicalOp.getCriteria()) {
				final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
				criteria.accept(filterBuilder);
				final String criteriaString = filterBuilder.criteriaBuilder.toString();
				if (!criteriaString.equals("")) {
					criteriaNodes.add(criteriaString);
				}
			}
			switch (logicalOp.getType()) {
			case NOT:
				criteriaBuilder.append("!(")
							   .append(criteriaNodes.get(0))
							   .append(")");
				break;
			case AND:
			case OR:
				Joiner.on(" " + SQLDSLConfig.getLogicalOpString(
						logicalOp.getType()) + " ")
					  .appendTo(criteriaBuilder, criteriaNodes);
				break;
			default:
				throw new UnsupportedOperationException(
						"There are not handlers for this logical operator" + logicalOp.getType());
			}
			criteriaBuilder.append(")");
			return new DefaultCriteriaVisitor();
		}
	}
}