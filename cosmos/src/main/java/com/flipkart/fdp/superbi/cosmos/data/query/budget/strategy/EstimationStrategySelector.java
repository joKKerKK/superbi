package com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import java.util.Map;

/**
 * A dependency injection which injects {@link BudgetEstimationStrategy}
 * for source. see {@link AbstractDSLConfig}
 */
public enum EstimationStrategySelector {
    instance;

    public BudgetEstimationStrategy select(Map<String, String> overrides) {
        if (!overrides.containsKey(AbstractDSLConfig.Attributes.QUERY_BUDGET_ESTIMATION))
            return new DefaultBudgetEstimationStrategy();
        String klass = overrides.get(AbstractDSLConfig.Attributes.QUERY_BUDGET_ESTIMATION);
        try {
            return (BudgetEstimationStrategy) Class.forName(klass).newInstance();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
