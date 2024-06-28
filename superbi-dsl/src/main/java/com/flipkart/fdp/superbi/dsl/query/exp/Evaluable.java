package com.flipkart.fdp.superbi.dsl.query.exp;

import com.google.common.base.Function;
import java.util.Map;

/**
 * User: shashwat
 * Date: 16/01/14
 */
public interface Evaluable<T> {
    T evaluate(Map<String, String[]> paramValues) throws ExprEvalException;

    class F {

        public static <T, E extends Evaluable<T>> EvalFn<T, E> evalFunc(Map<String, String[]> params) {
            return new EvalFn<T, E>(params);
        }

        public static class EvalFn<T, E extends Evaluable<T>> implements Function<E, T> {
            private final Map<String, String[]> params;

            private EvalFn(Map<String, String[]> params) {
                this.params = params;
            }

            @Override
            public T apply(E input) {
                try {
                    return input.evaluate(params);
                } catch (ExprEvalException e) {
                    throw new RuntimeException(e); // TODO: proper exception handling
                }
            }
        }
    }
}
