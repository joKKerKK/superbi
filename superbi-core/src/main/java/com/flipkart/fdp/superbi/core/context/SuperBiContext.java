package com.flipkart.fdp.superbi.core.context;

import com.flipkart.fdp.superbi.core.config.ApiKey;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 19/07/19
 */
@AllArgsConstructor
@Getter
public class SuperBiContext {
  private final String clientId;
  private final String userName;
  private final Optional<String> systemUser;
  private final ClientPrivilege clientPrivilege;

  public SuperBiContext(String clientId, String userName, ClientPrivilege clientPrivilege) {
    this.clientId = clientId;
    this.userName = userName;
    this.clientPrivilege = clientPrivilege;
    this.systemUser = Optional.empty();
  }
}
