package com.flipkart.fdp.superbi.dsl.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * User: aniruddha.gangopadhyay
 * Date: 11/03/14
 * Time: 3:05 PM
 */
public final class Funcs {

    public static class Futures {
        public static <T, F extends Future<T>> Function<F, T> realize() {
            return new RealizedFuture<T, F>();
        }

        public static <T> Function<Future<T>, T> realizeFuture() {
            return new RealizedFuture<T, Future<T>>();
        }

        public static <T> Function<Future<T>, T> realizeFuture(long timeout, TimeUnit unit) {
            return new RealizedFutureInTime<T, Future<T>>(unit.toMillis(timeout));
        }

        public static Predicate<? super Future<?>> completedFutures() {
            return new CompletedFuturePredicate();
        }

        public static class CompletedFuturePredicate implements Predicate<Future<?>> {
            public boolean apply(Future input) {
                return input.isDone();
            }
        }

        private static class RealizedFuture<T, F extends Future<T>> implements Function<F, T> {
            @Override
            public T apply(F input) {
                try {
                    return input.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private static class RealizedFutureInTime<T, F extends Future<T>> implements Function<F, T> {
            private long timeOutMs;

            public RealizedFutureInTime(long timeOutMs) {
                this.timeOutMs = timeOutMs;
            }

            @Override
            public T apply(F input) {
                final long start = System.currentTimeMillis();
                try {
                    return input.get(timeOutMs, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    final long end = System.currentTimeMillis();
                    final long elapsedMs = end - start;

                    timeOutMs = (timeOutMs > elapsedMs)? timeOutMs - elapsedMs : 0;
                }
            }
        }
    }


    public static class Arrays {
        public static Function<String[], String> getFirst() {
            return getFirst;
        }

        public static Function<String, String[]> mkArray() {
            return mkArray;
        }

        public static <T> T[] mkArray(T... values) {
            return values;
        }

        private static final Function<String[], String> getFirst = new Function<String[], String>() {
            @Override
            public String apply(String[] input) {
                return input[0];
            }
        };

        private static final Function<String, String[]> mkArray = new Function<String, String[]>() {
            @Override
            public String[] apply(String input) {
                return new String[]{input};
            }
        };
    }

    public static class Maps {

        public static <K, V> Function<K, V> forMap(Map<K, ? extends V> map, Function<K, V> defaultSupplier) {
            return new ForMapWithDefaultSupplier<K, V>(map, defaultSupplier);
        }

        private static class ForMapWithDefaultSupplier<K, V> implements Function<K, V>, Serializable {
            final Map<K, ? extends V> map;
            final Function<K, V> defaultSupplier;

            ForMapWithDefaultSupplier(Map<K, ? extends V> map, Function<K, V> defaultSupplier) {
                this.map = checkNotNull(map);
                this.defaultSupplier = checkNotNull(defaultSupplier);
            }

            @Override
            public V apply(@Nullable K key) {
                V result = map.get(key);
                return (result != null || map.containsKey(key)) ? result : defaultSupplier.apply(key);
            }

            @Override
            public boolean equals(@Nullable Object o) {
                if (o instanceof ForMapWithDefaultSupplier) {
                    ForMapWithDefaultSupplier<?, ?> that = (ForMapWithDefaultSupplier<?, ?>) o;
                    return map.equals(that.map) && Objects.equal(defaultSupplier, that.defaultSupplier);
                }
                return false;
            }
        }

        public static <K, V, V1> ImmutableMap<K, V1> extract(
            Iterable<V> values, Function<? super V, Map.Entry<K, V1>> keyValueFunction) {
            return extract(values.iterator(), keyValueFunction);
        }

        public static <K, V, V1> ImmutableMap<K, V1> extract(
            Iterator<V> values, Function<? super V, Map.Entry<K, V1>> keyValueFunction) {
            checkNotNull(keyValueFunction);
            ImmutableMap.Builder<K, V1> builder = ImmutableMap.builder();
            while (values.hasNext()) {
                V value = values.next();
                builder.put(keyValueFunction.apply(value));
            }
            return builder.build();
        }

        public static <K, V> Map<K, V> toMap(Iterable<Map.Entry<K, V>> entries) {
            final ImmutableMap.Builder<K, V> mapBuilder = ImmutableMap.builder();
            for (Map.Entry<K, V> entry : entries) {
                mapBuilder.put(entry);
            }
            return mapBuilder.build();
        }

    }

    private Funcs() {
    }
}
