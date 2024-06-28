package com.flipkart.fdp.superbi.refresher.dao.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import java.nio.ByteBuffer;
import lombok.Builder;
import lombok.Getter;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationType;

@Getter
public class HiveDriverHandle implements JsonSerializable {
  private final byte[] secret;
  private final byte[] guid;
  private final String host;
  private final int port;
  private final String user;

  @Builder
  @JsonCreator
  public HiveDriverHandle(@JsonProperty("secret")byte[] secret, @JsonProperty("guid")byte[] guid,
      @JsonProperty("host") String host, @JsonProperty("port")int port,@JsonProperty("user")String user) {
    this.secret = secret;
    this.guid = guid;
    this.host = host;
    this.port = port;
    this.user = user;
  }

  @JsonIgnore
  public TOperationHandle getAsTOperationHandle() {
    return new TOperationHandle(
        new THandleIdentifier(
            ByteBuffer.wrap(this.getGuid()),
            ByteBuffer.wrap(this.getSecret())
        ),
        TOperationType.EXECUTE_STATEMENT,
        true);

  }
}
