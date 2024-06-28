package com.flipkart.fdp.superbi.cosmos.exception;

/**
 * Created by sridhar.dhanush on 10/01/17.
 */
public class CompositeTypeNotRegisteredException extends RuntimeException {
    public CompositeTypeNotRegisteredException(String errorMessage) {
        super(errorMessage);
    }

}
