package com.flipkart.fdp.superbi.mail;


import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class MailResponse {

  String message;
  Boolean isSuccess;

}
