package com.flipkart.fdp.superbi.cosmos.exception;

/**
 * Created by arun.khetarpal on 16/09/15.
 */
public class QueryBudgetExceededException extends QueryExecutionException {
    private String feedback;

    public QueryBudgetExceededException() {
    }

    public QueryBudgetExceededException(String message, String feedback) {
        super(message);
        this.feedback = feedback;
    }

    public QueryBudgetExceededException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryBudgetExceededException(Throwable cause) {
        super(cause);
    }

    public String getFeedback() {
        return feedback;
    }
}
