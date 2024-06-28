package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.cosmos.data.ExecutionEventType;
import com.flipkart.fdp.superbi.cosmos.data.query.QuerySubmitResult;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.EstimatedBudgetType;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.QueryBudgetEstimation;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.QueryBudgetResult;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import com.flipkart.fdp.superbi.cosmos.exception.QueryBudgetExceededException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by arun.khetarpal on 23/09/15.
 */
public class BudgetedPolicyExecutor<T extends QuerySubmitResult> implements Executable {
    private static final Logger logger = LoggerFactory.getLogger(BudgetedPolicyExecutor.class);

    private Executable<T> decoratedExecutable;
    private long elapsedTime;


    public BudgetedPolicyExecutor(Executable<T> executable) {
        this.decoratedExecutable = executable;
    }

    @Override
    public long elapsedTimeMs() {
        return elapsedTime + decoratedExecutable.elapsedTimeMs();
    }

    @Override
    public Optional<T> execute(ExecutionContext context, NativeQueryTranslator translator) {
        Timer elapsedTimer = new Timer().start();


        QueryBudgetResult budget = new QueryBudgetResult.Builder().setEstimatedBudgetType
            (EstimatedBudgetType.NOOPT).
                setEstimatorUsed(this.getClass().getName()).build();
        boolean queryInBudget = true;

        try {
            QueryBudgetEstimation estimator = new QueryBudgetEstimation(context);
            budget = estimator.calculate();

        } catch (Exception ex) {
            logger.info("Cannot calculate budget: {} ", ex.getMessage());
            ExecutionEvent event = new ExecutionEvent(ExecutionEventType
                .BUDGET_CALCULATION_FAILED, ex);
            context.publishEvent(event, this);
        } finally {
            logger.info("Budget of query is: {}", budget.getBudgetType().toString());
            ExecutionEvent event = new ExecutionEvent(ExecutionEventType
                .BUDGET_CALCULATION_COMPLETE, budget);
            context.publishEvent(event, this);

            elapsedTimer.stop();
            elapsedTime = elapsedTimer.getTimeTakenMs();
        }

        if (queryInBudget)
            return decoratedExecutable.execute(context, translator);

        throw new QueryBudgetExceededException("Query is too expensive to run on source", budget.toJson());
    }
}
