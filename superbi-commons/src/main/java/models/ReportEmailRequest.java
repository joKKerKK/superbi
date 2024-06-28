package models;

import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReportEmailRequest {

  List<String> toAddressList;
  String reportName;
  String reportLink;
  String d42Link;
  Date expiryDate;
  String subscriptionName;
  long subscriptionId;
  ByteArrayOutputStream dataStream;
  String exception;
  Integer numberOfRows;
  String deliveryType;
  String deliveryOption;
  Boolean isTruncated;
  Boolean isOTS;
}
