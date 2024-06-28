package com.flipkart.fdp.superbi.core.config;

import com.flipkart.fdp.superbi.entities.ReportAction;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class ClientPrivilege {
  private ReportAction reportAction;
  private DataPrivilege dataPrivilege;
}
