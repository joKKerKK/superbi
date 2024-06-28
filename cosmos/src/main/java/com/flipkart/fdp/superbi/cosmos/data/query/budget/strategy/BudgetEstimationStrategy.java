package com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy;


import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.QueryBudgetResult;


/**
 * All estimations should be derived from this class.
 * The {@link #calculate(ExecutionContext)} method should return the
 * the query budget.
 */
public interface BudgetEstimationStrategy {
    QueryBudgetResult calculate(ExecutionContext context);
}
