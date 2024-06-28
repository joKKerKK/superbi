package com.flipkart.fdp.superbi.refresher.api.cache.impl.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by akshaya.sharma on 22/07/19
 */
@AllArgsConstructor
@Getter
@Slf4j
@Builder
public class CouchbaseBucketConfig {

  private final String[] nodes;
  private final String bucketName;
  private final String bucketPassword;
  private final int obsTimeoutMs;
  private final int opTimeoutMs;
  private final int OpQueueMaxBlockTimeMS;

  public CouchbaseBucketConfig(String[] nodes, String bucketName,
      String bucketPassword) {
    this(nodes, bucketName, bucketPassword, 0, 0, 0);
  }


  public final Bucket getCouchbaseBucket() throws IOException {
    try {
      CouchbaseCluster cluster = CouchbaseCluster.create(nodes);
      return cluster.openBucket(bucketName, bucketPassword);
    } catch (Exception e) {
      log.error("Error connecting to Couchbase for bucket {}", getBucketName(), e);
      throw e;
    }
  }
}
