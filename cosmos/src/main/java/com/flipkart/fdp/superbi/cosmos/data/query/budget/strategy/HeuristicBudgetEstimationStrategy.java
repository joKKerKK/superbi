package com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.ExecutorFacade;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.Executable;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutorPolicyCreatorImpl;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.NativeQueryTranslator;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.CardinalityQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.EstimatedBudgetType;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.QueryBudgetResult;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This estimator converts a user specified {@link DSQuery} and tries to
 * see the estimated cardinality with respects to group by.
 *
 * The estimates of budget is made as follows
 * <table style="width:100%">
 *      <tr>
 *          <th>Cardinality</th>
 *          <th>Type</th>
 *      </tr>
 *      <tr>
 *          <th>Greater than threshold * 3.0 </th>
 *          <th>{@link EstimatedBudgetType#WREAKINGBALL}</th>
 *      </tr>
 *      <tr>
 *          <th>Greater than threshold</th>
 *          <th>{@link EstimatedBudgetType#BOWLINGBALL}</th>
 *      </tr>
 *      <tr>
 *          <th>Greater than threshold * 0.5</th>
 *          <th>{@link EstimatedBudgetType#GOLFBALL}</th>
 *      </tr>
 *      <tr>
 *          <th>Less than threshold * 0.5</th>
 *          <th>{@link EstimatedBudgetType#FEATHER}</th>
 *      </tr>
 *</table>
 */
public class HeuristicBudgetEstimationStrategy implements BudgetEstimationStrategy {
    private static final Logger logger = LoggerFactory.getLogger(HeuristicBudgetEstimationStrategy.class);

    public HeuristicBudgetEstimationStrategy() {

    }

    /**
     * @param qr
     * @return Cardinality is returned. Need BigDecimal because the cross-product cardinality can
     * be really large
     */
    private BigDecimal calculateCardinality(QueryResult qr) {
        BigDecimal cardinality = new BigDecimal(1);

        for (ResultRow resultRow : qr.data) {
            for (Object col : resultRow.row) {
                cardinality = cardinality.multiply(BigDecimal.valueOf(((Number) col).doubleValue()));
            }
        }
        return cardinality;
    }

    private Map<String, Double> createDistinctValueMapping(DSQuery query, QueryResult results) {
        if (results.data.size() != 1) return null;
        Map<String, Double> distinctMapping = new HashMap<>();
        List<String> persistentColumns = Lists.newArrayList(Iterables.transform(query.getNonDerivedColumns(), SelectColumn.F.name));
        int offset = 0;
        for (Object col : results.data.get(0).row) {
            String columnName = persistentColumns.get(offset++);
            if (columnName.startsWith("DISTINCT_COUNT_"))
                columnName = columnName.replaceFirst("DISTINCT_COUNT_","");
            distinctMapping.put(columnName, ((Number) col).doubleValue());
        }
        return distinctMapping;
    }

    @Override
    public QueryBudgetResult calculate(ExecutionContext context) {

        if (!context.getQuery().hasGroupBys())
            return new QueryBudgetResult.Builder().setEstimatedBudgetType(EstimatedBudgetType.NOOPT).
                    setEstimatorUsed(this.getClass().getSimpleName()).build();

        long threshold = context.getConfig().getBudgetThreshold();

        DSQuery query = context.getQuery();
        Map<String, String[]> params = context.getParams();
        AbstractDSLConfig config = context.getConfig();

        CardinalityQueryBuilder cardinalityQueryBuilder = new CardinalityQueryBuilder(query, params, config);
        query.accept(cardinalityQueryBuilder);

        final DSQuery cardinatityQuery = cardinalityQueryBuilder.build();

        Timer totalTimer = new Timer().start();
        ExecutionContext currContext = new ExecutionContext.Builder().setDSQuery(cardinatityQuery).setParams(params).
                setCacheClient(context.getQueryResultStoreOptional())
                .setFederationType(context.getFederationType()).build();

        logger.info("Calculating budget using HeuristicBudgetEstimationStrategy");

        NativeQueryTranslator translator = new NativeQueryTranslator(currContext);

        Executable executor = ExecutorPolicyCreatorImpl.instance.getBasicPolicy(currContext);
        QueryResult qr = ExecutorFacade.instance.execute(currContext, translator, executor, totalTimer);

        BigDecimal cardinality = calculateCardinality(qr);

        logger.info("Cardinality: {} Threshold: {}", cardinality, threshold);

        EstimatedBudgetType estimate = EstimatedBudgetType.NOOPT;
        if (cardinality.compareTo(BigDecimal.valueOf(threshold * 3)) > 0) {
            estimate = EstimatedBudgetType.WREAKINGBALL;
        } else if (cardinality.compareTo(BigDecimal.valueOf(threshold)) > 0) {
            estimate = EstimatedBudgetType.BOWLINGBALL;
        } else if (cardinality.compareTo(BigDecimal.valueOf(threshold * .5)) > 0) {
            estimate = EstimatedBudgetType.GOLFBALL;
        } else {
            estimate = EstimatedBudgetType.FEATHER;
        }

        return new QueryBudgetResult.Builder().setEstimatedBudgetType(estimate).setEstimatorUsed(this.getClass().getSimpleName())
                .setData("_threshold", threshold).setData("_cardinality", cardinality)
                .setData("_distinct_values", createDistinctValueMapping(cardinatityQuery, qr))
                .build();
    }
}
