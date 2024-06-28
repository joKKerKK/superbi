package com.flipkart.fdp.superbi.refresher.dao.hdfs;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class HdfsConfig {

  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final int recoveryTimeOutLimitMs;
  private final String queue;
  private final String priorityClient;
  private final List<String> initScripts;

}
