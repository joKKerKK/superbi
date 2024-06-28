package com.flipkart.fdp.superbi.dsl.query;

/**
 * User: shashwat
 * Date: 24/01/14
 */
public interface Representation {
    <T> T from(String representation, Class<T> klass);
}
