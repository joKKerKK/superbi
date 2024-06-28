package com.flipkart.fdp.superbi.models;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@Getter
@EqualsAndHashCode
/**
 * This is a supporting wrapper class to be able to serde any NativeQuery.
 * The serde of this class uses "ClassName" of the underlying `query`
 */
public class NativeQuery {
  // Ideally it should be a JacksonSerializable and Deserializable
  @NonNull
  private final Object query;

  public NativeQuery(Object query) {
    this.query = query;
  }

  public String getNativeQueryClassName() {
    return getQuery().getClass().getCanonicalName();
  }
}
