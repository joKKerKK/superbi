package com.flipkart.fdp.superbi.mail;

import models.ReportEmailRequest;

public interface EmailClient {

  MailResponse sendMail(ReportEmailRequest reportEmailRequest);

  MailResponse sendFailureEmail(ReportEmailRequest reportEmailRequest);

  MailResponse sendCommEmail(ReportEmailRequest reportEmailRequest);
}
