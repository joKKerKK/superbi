package com.flipkart.fdp.superbi.cosmos.data.query.budget;

import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy.BudgetEstimationStrategy;

/**
 * Created by arun.khetarpal on 15/09/15.
 */
public class QueryBudgetEstimation {

    ExecutionContext context;

    public QueryBudgetEstimation(ExecutionContext context) {
        this.context = context;
    }

    @LogExecTime
    public QueryBudgetResult calculate() {
        BudgetEstimationStrategy estimationStrategy = context.getConfig().getEstimationStrategy();
        return estimationStrategy.calculate(context);
    }
}
