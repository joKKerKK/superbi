package com.flipkart.fdp.superbi.cosmos.cache.cacheCore;


import java.util.Optional;

/**
 * Created by piyush.mukati on 13/05/15.
 * contract for  CacheClient implementation
 */
public interface ICacheClient<key, value> {

    Optional<value> get(key k);

    boolean set(key k, int ttl, value v);

    default boolean add(key k, int ttl, value v) {
        if (get(k).isPresent()) {
            return false;
        } else {
            return set(k, ttl, v);
        }
    }

    boolean remove(key k);


}
