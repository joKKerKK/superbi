package com.flipkart.fdp.superbi.mail;

import com.flipkart.fdp.superbi.constants.EmailConstants;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import models.ReportEmailRequest;
import org.apache.commons.lang3.text.StrSubstitutor;

@AllArgsConstructor
@Slf4j
public class DefaultEmailClient implements EmailClient{

  public static final String FILE_FORMAT = ".csv";
  private final Session session;
  private final String emailTemplate;
  private final String gsheetCreationEmailTemplate;
  private final String gsheetCancelledEmailTemplate;
  private final String gsheetOverwriteTemplate;
  private final String failureEmailTemplate;
  private final String expirationCommTemplate;

  @Override
  public MailResponse sendMail(ReportEmailRequest reportEmailRequest) {
    try{
      //copied from hydra
      MimeMessage mimeMessage = initializeMimeMessage(reportEmailRequest);
      mimeMessage.setSubject(reportEmailRequest.getSubscriptionName());
      BodyPart htmlPart = new MimeBodyPart();
      htmlPart.setContent(generateMessageText(reportEmailRequest), "text/html");
      Multipart mimeMultipart = setContent(htmlPart, mimeMessage);
      if (reportEmailRequest.getDataStream() != null) {
        MimeBodyPart mbp2 = new MimeBodyPart();
        DataSource attachment = new ByteArrayDataSource(reportEmailRequest.getDataStream().toByteArray(),"text/csv");
        mbp2.setDataHandler(new DataHandler(attachment));
        mbp2.setFileName(reportEmailRequest.getReportName() + FILE_FORMAT);
        mimeMultipart.addBodyPart(mbp2);
      }
      Transport.send(mimeMessage);
      log.info(MessageFormat.format("Mail sent successful for subscriptionId {0}"
          ,reportEmailRequest.getSubscriptionId()));
      return MailResponse.builder().isSuccess(true).build();
    }catch (Exception e){
      log.error(MessageFormat.format("Mail sent failed for subscriptionId {0}"
          ,reportEmailRequest.getSubscriptionId()));
      return MailResponse.builder().isSuccess(false).message(e.getMessage()).build();
    }
  }

  @Override
  public MailResponse sendFailureEmail(ReportEmailRequest reportEmailRequest) {
    try {
      MimeMessage mimeMessage = initializeMimeMessage(reportEmailRequest);
      mimeMessage.setSubject(reportEmailRequest.getSubscriptionName());
      BodyPart htmlPart = new MimeBodyPart();
      htmlPart.setContent(generateFailureMessageText(reportEmailRequest), "text/html");
      setContent(htmlPart, mimeMessage);
      Transport.send(mimeMessage);
      log.info(MessageFormat.format("Failure Mail sent successful for subscriptionId {0}"
          ,reportEmailRequest.getSubscriptionId()));
      return MailResponse.builder().isSuccess(true).build();
    } catch (Exception e) {
      log.error(MessageFormat.format("Failure Mail sent failed for subscriptionId {0}"
          ,reportEmailRequest.getSubscriptionId()));
      return MailResponse.builder().isSuccess(false).message(e.getMessage()).build();
    }
  }

  private MimeMessage initializeMimeMessage(ReportEmailRequest reportEmailRequest)
      throws MessagingException {
    MimeMessage mimeMessage = new MimeMessage(session);
    mimeMessage.setFrom(new InternetAddress("bigfoot-reporting@flipkart.com"));
    InternetAddress[] address = new InternetAddress[reportEmailRequest.getToAddressList().size()];
    for (int i = 0; i < reportEmailRequest.getToAddressList().size(); i++) {
      address[i] = new InternetAddress(reportEmailRequest.getToAddressList().get(i));
    }
    mimeMessage.setRecipients(Message.RecipientType.TO, address);
    return mimeMessage;
  }

  private String generateMessageText(ReportEmailRequest reportEmailRequest){
    HashMap<String,String> channelDataModel = new HashMap<>();
    channelDataModel.put(EmailConstants.EMAIL_SUBJECT, reportEmailRequest.getSubscriptionName());
    channelDataModel.put(EmailConstants.REPORT_NAME, reportEmailRequest.getReportName());
    channelDataModel.put(EmailConstants.D42_LINK,reportEmailRequest.getD42Link());
    String pattern = "MMM dd HH:mm:ss yyyy";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    channelDataModel.put(EmailConstants.EXPIRY_DATE, simpleDateFormat.format(reportEmailRequest.getExpiryDate()));
    channelDataModel.put(EmailConstants.ROW_COUNT, reportEmailRequest.getNumberOfRows().toString());
    String subscriptionType = (reportEmailRequest.getIsOTS()) ? EmailConstants.DOWNLOAD_MAIL :
        EmailConstants.SUBSCRIPTION_MAIL;
    channelDataModel.put(EmailConstants.SUBSCRIPTION_TYPE, subscriptionType);
    StrSubstitutor substitutor = new StrSubstitutor(channelDataModel);
    String template = getTemplate(reportEmailRequest);
    String messageText = substitutor.replace(template);
    return messageText;
  }

  private String generateFailureMessageText(ReportEmailRequest reportEmailRequest){
    HashMap<String,String> channelDataModel = new HashMap<>();
    channelDataModel.put(EmailConstants.EMAIL_SUBJECT, reportEmailRequest.getSubscriptionName());
    channelDataModel.put(EmailConstants.REPORT_NAME, reportEmailRequest.getReportName());
    channelDataModel.put(EmailConstants.EXCEPTION,reportEmailRequest.getException());
    StrSubstitutor substitutor = new StrSubstitutor(channelDataModel);
    String messageText = substitutor.replace(failureEmailTemplate);
    return messageText;
  }

  private String getTemplate(ReportEmailRequest reportEmailRequest){
    if (reportEmailRequest.getDeliveryType().equals("D42")) {
      return emailTemplate;
    } else if (reportEmailRequest.getDeliveryType().equals("GSHEET") && reportEmailRequest.getIsTruncated()) {
      return gsheetCancelledEmailTemplate;
    } else if (reportEmailRequest.getDeliveryType().equals("GSHEET") && reportEmailRequest.getDeliveryOption().equals("OVERWRITE")) {
      return gsheetOverwriteTemplate;
    } else {
      return gsheetCreationEmailTemplate;
    }
  }

  @Override
  public MailResponse sendCommEmail(ReportEmailRequest reportEmailRequest) {
    try {
      MimeMessage mimeMessage = initializeMimeMessage(reportEmailRequest);
      mimeMessage.setSubject(String.format("[Action Required] Your subscription %s is about to expire", reportEmailRequest.getSubscriptionName()));
      BodyPart htmlPart = new MimeBodyPart();
      htmlPart.setContent(generateExpirationCommText(reportEmailRequest), "text/html");
      setContent(htmlPart, mimeMessage);
      Transport.send(mimeMessage);
      log.info(
          MessageFormat.format("Comm Mail sent successful for subscriptionId {0}"
              , reportEmailRequest.getSubscriptionId()));
      return MailResponse.builder().isSuccess(true).build();
    } catch (Exception e) {
      log.error(MessageFormat.format("Subscription Expiry Comm Mail sent failed for subscriptionId {0} due to {1}"
          , reportEmailRequest.getSubscriptionId(), e.getMessage()));
      return MailResponse.builder().isSuccess(false).message(e.getMessage()).build();
    }
  }

  private Multipart setContent(BodyPart htmlPart, MimeMessage mimeMessage) throws javax.mail.MessagingException {
      Multipart multipart = new MimeMultipart();
      multipart.addBodyPart(htmlPart);
      MimeBodyPart mimeBodyPart = new MimeBodyPart();
      mimeBodyPart.setContent(multipart);
      Multipart mimeMultipart = new MimeMultipart();
      mimeMultipart.addBodyPart(mimeBodyPart);
      mimeMessage.setContent(mimeMultipart);
      mimeMessage.setSentDate(new Date());
      return mimeMultipart;
  }

  private String generateExpirationCommText(ReportEmailRequest reportEmailRequest) {
    HashMap<String, String> channelDataModel = new HashMap<>();
    String pattern = "MMM dd HH:mm:ss yyyy";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    channelDataModel.put(EmailConstants.EMAIL_SUBJECT, reportEmailRequest.getSubscriptionName());
    channelDataModel.put(EmailConstants.REPORT_NAME, reportEmailRequest.getReportName());
    channelDataModel.put(EmailConstants.EXPIRY_DATE, simpleDateFormat.format(reportEmailRequest.getExpiryDate()));
    StrSubstitutor substitutor = new StrSubstitutor(channelDataModel);
    return substitutor.replace(expirationCommTemplate);
  }

}
