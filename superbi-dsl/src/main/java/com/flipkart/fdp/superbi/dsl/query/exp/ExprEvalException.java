package com.flipkart.fdp.superbi.dsl.query.exp;

/**
 * User: shashwat
 * Date: 22/01/14
 */
public class ExprEvalException extends Exception {

    public ExprEvalException(String message) {
        super(message);
    }

    public ExprEvalException(String message, Throwable cause) {
        super(message, cause);
    }
}
