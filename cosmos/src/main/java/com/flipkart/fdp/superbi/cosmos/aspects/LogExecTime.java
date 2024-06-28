package com.flipkart.fdp.superbi.cosmos.aspects;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by rajesh.kannan on 19/06/15.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD})
public @interface LogExecTime {
}
