package com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.EstimatedBudgetType;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.QueryBudgetResult;

/**
 * The {@link BudgetEstimationStrategy} which returns an unimplimented budget
 */
public class DefaultBudgetEstimationStrategy implements BudgetEstimationStrategy {
    @Override
    public QueryBudgetResult calculate(ExecutionContext context) {
        return new QueryBudgetResult.Builder().setEstimatedBudgetType(EstimatedBudgetType.NOOPT).
                setEstimatorUsed(this.getClass().getSimpleName()).build();
    }
}
