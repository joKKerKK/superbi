package com.flipkart.fdp.superbi.subscription.util;

public class StringUtil {

  static final int DEFAULT_LENGTH = 10000;

  public static String shortenString(String input,int size){
    if(input == null){
      return input;
    }
    Integer finalSize = size > input.length() ? input.length() : size;
    return input.substring(0,finalSize);
  }

  public static String shortenString(String input){
    return shortenString(input,DEFAULT_LENGTH);
  }
}
