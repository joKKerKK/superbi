package com.flipkart.fdp.superbi.core.util;

/**
 * Created by rajesh.kannan on 21/12/14.
 */
public class FunctionUtils {
    public interface Block<T> {
        void apply(T input);
    }


    public static <T> void forEach(Iterable<? extends T> iterable,
                                   Block<? super T> block) {
        for (T element : iterable) {
            block.apply(element);
        }
    }


}
