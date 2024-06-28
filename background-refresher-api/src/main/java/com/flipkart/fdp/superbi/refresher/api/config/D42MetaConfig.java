package com.flipkart.fdp.superbi.refresher.api.config;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class D42MetaConfig {

  private final long d42ExpiryInSeconds = 604800;
  private final List<String> d42UploadClients = new ArrayList<>();
  private final int maxSizePerChunk;
  private final int maxRowSize;

}
