package com.flipkart.fdp.superbi.mail;

import com.flipkart.fdp.JsonUtil;
import com.flipkart.fdp.superbi.constants.EmailConstants;
import com.google.inject.Inject;
import emailsvc.CommunicationService;
import emailsvc.Connekt.ConnektEmailCommunicationRequest;
import emailsvc.Connekt.ConnektServiceConfig;
import emailsvc.Connekt.MailChannelInfo;
import emailsvc.EmailCommunicationRequest;
import emailsvc.EmailId;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import models.ReportEmailRequest;

@Slf4j
public class ConnektEmailClient implements EmailClient{

  public static final String MAIL_STATUS = "status";
  public static final String MAIL_SUCESS_CODE = "202";
  private final CommunicationService communicationService;
  private final ConnektServiceConfig serviceConfig;

  @Inject
  public ConnektEmailClient(CommunicationService communicationService,ConnektServiceConfig serviceConfig){
    this.communicationService = communicationService;
    this.serviceConfig = serviceConfig;
  }

  @Override
  @SneakyThrows
  public MailResponse sendMail(ReportEmailRequest reportEmailRequest) {
    List<EmailId> emailIdList = reportEmailRequest.getToAddressList().stream()
        .map(email-> EmailId.builder().address(email).name(email).build()).collect(
            Collectors.toList());
    MailChannelInfo mailChannelInfo = MailChannelInfo.builder().to(emailIdList).build();
    HashMap<String,String> channelDataModel = new HashMap<>();
    channelDataModel.put(EmailConstants.EMAIL_SUBJECT, reportEmailRequest.getReportName());
    channelDataModel.put(EmailConstants.REPORT_NAME, reportEmailRequest.getReportName());
    channelDataModel.put(EmailConstants.D42_LINK,reportEmailRequest.getD42Link());
    String pattern = "MMM dd HH:mm:ss yyyy";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    channelDataModel.put(EmailConstants.EXPIRY_DATE, simpleDateFormat.format(reportEmailRequest.getExpiryDate()));
    ConnektEmailCommunicationRequest connektEmailCommunicationRequest = ConnektEmailCommunicationRequest.builder()
        .stencilId(serviceConfig.getEmailStencilId())
        .contextId(serviceConfig.getContextId())
        .sla(serviceConfig.getSla())
        .channelInfo(mailChannelInfo)
        .channelDataModel(channelDataModel)
        .build();
    EmailCommunicationRequest emailCommunicationRequest = connektEmailCommunicationRequest;
    Map<String,Object> response = communicationService.sendEmail(emailCommunicationRequest);
    return MailResponse.builder().isSuccess(response.get(MAIL_STATUS).equals(MAIL_SUCESS_CODE))
        .message(JsonUtil.toJsonString(response)).build();
  }

  @Override
  public MailResponse sendFailureEmail(ReportEmailRequest reportEmailRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public MailResponse sendCommEmail(ReportEmailRequest reportEmailRequest) {
    throw new RuntimeException("Not implemented");
  }
}
