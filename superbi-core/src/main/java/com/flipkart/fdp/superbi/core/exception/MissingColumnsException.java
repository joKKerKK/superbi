package com.flipkart.fdp.superbi.core.exception;

import com.google.common.collect.Lists;
import java.text.MessageFormat;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 31/07/19
 */
@AllArgsConstructor
public class MissingColumnsException extends RuntimeException{
  private static final String MESSAGE_TEMPLATE = "Underlying fact \"{0}\" does have columns \"{1}\". Please remove them from the report";
  private final String factName;
  private final List<String> missingColumns;

  @Override
  public String getMessage() {
    return MessageFormat.format(MESSAGE_TEMPLATE, factName, StringUtils.join(missingColumns.toArray(), ","));
  }
}
