package com.flipkart.fdp.superbi.cosmos.data.api.execution.bq;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getDefaultTimeZone;
import static java.time.temporal.TemporalAdjusters.next;
import static java.time.temporal.TemporalAdjusters.previous;

import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

public class BQUtils {
  public static final String SELECT = "select";
  public static final String FROM = "from";
  public static final String FRACTILE_GROUP_BY_HOLDER = "{{FRACTILE_GROUP_BYS}}";
  public static final String FRACTILE_PARTITION_BY_HOLDER = "PARTITION BY " + FRACTILE_GROUP_BY_HOLDER;
  public static final String DISTINCT  = "distinct ";
  public static final String OPEN_PARENTHESIS = "(";
  public static final String CLOSE_PARENTHESIS = ")";
  public static final String OPEN_BOX_PARENTHESIS = "[";
  public static final String CLOSE_BOX_PARENTHESIS = "]";
  public static final String WHERE = "where";
  public static final String GROUP_BY = "group by";
  public static final String ORDER_BY = "order by";
  public static final String LIMIT = "limit";
  public static final String PERCENTILE_CONT = "Percentile_Cont";
  public static final String OVER = "OVER";
  public static final String RANGE_BUCKET = "range_bucket";
  public static final String TIMESTAMP = "TIMESTAMP";
  public static final String UNIX_MILLIS = "UNIX_MILLIS";
  public static final String DATE = "DATE";
  public static final String ARRAY_CONCAT = "ARRAY_CONCAT";
  public static final String MONTH = "MONTH";
  public static final String WEEK = "WEEK";
  public static final String INTERVAL = "INTERVAL";
  public static final String MILLISECOND = "MILLISECOND";
  public static final String GENERATE_TIMESTAMP_ARRAY = "GENERATE_TIMESTAMP_ARRAY";
  public static final String GENERATE_ARRAY = "GENERATE_ARRAY";
  public static final String GENERATE_DATE_ARRAY = "GENERATE_DATE_ARRAY";
  public static final String AS = "as";
  private static final String DATEDIFF = "datediff";
  private static final String DATETIME_DIFF = "datetime_diff";
  private static final String NVL = "nvl";
  private static final String COALESCE = "COALESCE";
  private static final String TIMESTAMPDIFF = "timestampdiff";
  private static final String TIMESTAMP_DIFF = "TIMESTAMP_DIFF";
  private static final String APPROXIMATE_COUNT_DISTINCT = "approximate_count_distinct";
  private static final String APPROX_COUNT_DISTINCT = "APPROX_COUNT_DISTINCT";
  private static final String VARCHAR = "varchar";
  private static final String TO_CHAR = "to_char";
  private static final String STRING_NATIVE = "STRING";
  private static final String POWER_VERTICA = "^";
  private static final String TO_DATE = "to_date";
  private static final String TO_TIMESTAMP = "to_timestamp";
  private static final String DAYOFWEEK = "dayofweek";
  private static final String VERTICA_SUB = "vertica_sub";
  private static final String PARSE_DATE = "parse_date";
  private static final String FORMAT_DATE = "FORMAT_DATE";
  private static final String PARSE_TIMESTAMP = "parse_timestamp";
  private static final String EXTRACT = "EXTRACT";
  private static final String CASE_INSENSITIVE = "(?i)";
  private static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
  private static final String CURRENT_DATETIME = "CURRENT_DATETIME";
  private static final String TIME_FORMAT_BQ = "'%Y%m%d'";
  private static final HashMap<String, String> timeFormatMapping =
      new HashMap<String, String>() {{
        put("YYYY", "%Y");
        put("HH24", "%H");
        put("MI", "%M");
        put("SS", "%S");
        put("MM", "%m");
        put("DD", "%d");
        put("HH12", "%I");
      }};
  public static final HashMap<String, Function<String,String>> diffNativeExp =
      new HashMap<String, Function<String,String>>() {{
    put(DATEDIFF, BQUtils::rectifyDateDiff);
    put(NVL, BQUtils::rectifyNvl);
    put(TIMESTAMPDIFF, BQUtils::rectifyTimestampDiff);
    put(APPROXIMATE_COUNT_DISTINCT, BQUtils::rectifyAppropriateCountDistinct);
    put(VARCHAR, BQUtils::rectifyVarchar);
    put(POWER_VERTICA, BQUtils::rectifyBitwiseOperator);
    put(TO_CHAR, BQUtils::rectifyToChar);
    put(TO_DATE, BQUtils::rectifyToDate);
    put(TO_TIMESTAMP, BQUtils::rectifyToTimestamp);
    put(DAYOFWEEK, BQUtils::rectifyDayOfWeek);


  }};


  public static String quote(String s) {
    if(StringUtils.isEmpty(s)){
      return s;
    }
    return String.format("`%s`", s);
  }

  public static String getWrappedAlias(String name){
    if(StringUtils.isEmpty(name)){
      return name;
    }
    return String.format(" as `%s`",name);
  }

  public static String addParenthesis(String s){
    if(StringUtils.isEmpty(s)){
      return s;
    }
    return String.format("(%s)", s);
  }

  public static String rectifyNativeExpressions(String s){
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String updatedExpression = s;
    for (String key : diffNativeExp.keySet()) {
      boolean isDiffNative = s.toLowerCase().contains(key);
      updatedExpression = isDiffNative ? diffNativeExp.get(key).apply(updatedExpression) : updatedExpression;
    }
    return updatedExpression;
  }

  public static String replaceTimestampWithDatetime(String s) {
    String timeStamp = CURRENT_TIMESTAMP;
    String dateTime = CURRENT_DATETIME;
    String rectified = s.replaceAll(CASE_INSENSITIVE + timeStamp, dateTime);
    return rectified;
  }

  private static String rectifyDateDiff(String s){
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = DATEDIFF + "\\(\\W?(.*?)\\W?,";
    String replacement = DATEDIFF + "\\($1,";
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    int numberOfArguments = 3;
    int numberOfOccurences = countPattern(rectified, Pattern.compile(DATEDIFF, Pattern.CASE_INSENSITIVE));
    for(int count = 1; count <= numberOfOccurences ;count++) {
      List<String> arguments = getArguments(rectified, DATEDIFF, numberOfArguments);
      if (arguments.isEmpty()) {
        break;
      }
      List<Integer> boundary = getFunctionBoundaries(rectified, DATEDIFF);
      String original = rectified.substring(boundary.get(0),boundary.get(1) + 1);
      StringBuilder changed = new StringBuilder(DATETIME_DIFF + "(");
      for (int i = 1; i <= numberOfArguments; i++) {
        if (i != numberOfArguments) {
          changed.append("datetime(");
        }
        changed.append(arguments.get(numberOfArguments - i));
        if (i != numberOfArguments) {
          changed.append(")");
          changed.append(",");
        }
      }
      changed.append(")");
      rectified = rectified.replace(original, changed.toString());
    }
    return rectified;
  }

  private static String rectifyNvl(String s) {
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = NVL;
    String replacement = COALESCE;
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    return rectified;
  }

  private static String rectifyTimestampDiff(String s) {
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = TIMESTAMPDIFF + "\\(\\W?(.*?)\\W?,";
    String replacement = TIMESTAMPDIFF + "\\($1,";
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    int numberOfArguments = 3;
    int numberOfOccurences = countPattern(rectified, Pattern.compile(TIMESTAMPDIFF, Pattern.CASE_INSENSITIVE));
    for(int count = 1; count <= numberOfOccurences ;count++) {
      List<String> arguments = getArguments(rectified, TIMESTAMPDIFF, numberOfArguments);
      if (arguments.isEmpty()) {
        break;
      }
      List<Integer> boundary = getFunctionBoundaries(rectified, TIMESTAMPDIFF);
      String original = rectified.substring(boundary.get(0),boundary.get(1) + 1);
      StringBuilder changed = new StringBuilder(TIMESTAMP_DIFF + "(");
      for (int i = 1; i <= numberOfArguments; i++) {
        if (i != numberOfArguments) {
          changed.append("datetime(");
        }
        changed.append(arguments.get(numberOfArguments - i));
        if (i != numberOfArguments) {
          changed.append(")");
          changed.append(",");
        }
      }
      changed.append(")");
      rectified = rectified.replace(original, changed.toString());
    }
    return rectified;
  }

  private static String rectifyAppropriateCountDistinct(String s) {
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = APPROXIMATE_COUNT_DISTINCT;
    String replacement = APPROX_COUNT_DISTINCT;
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    return rectified;
  }

  private static String rectifyVarchar(String s) {
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = VARCHAR;
    String replacement = STRING_NATIVE;
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    regexTarget = STRING_NATIVE + "\\(\\d*\\)";
    rectified = rectified.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    return rectified;
  }

  private static String rectifyToChar(String s) {
    if(StringUtils.isEmpty(s)){
      return s;
    }

    String regexTarget = TO_CHAR;
    String replacement = TO_CHAR;
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    int numberOfArguments = 2;
    int numberOfOccurences = countPattern(rectified, Pattern.compile(TO_CHAR, Pattern.CASE_INSENSITIVE));
    for(int count = 1; count <= numberOfOccurences ;count++) {
      List<String> arguments = getArguments(rectified, TO_CHAR, numberOfArguments);
      if (arguments.isEmpty()) {
        break;
      }
      arguments.set(1, checkTimeFormat(arguments.get(1)));
      List<Integer> boundary = getFunctionBoundaries(rectified, TO_CHAR);
      String original = rectified.substring(boundary.get(0),boundary.get(1) + 1);
      StringBuilder changed = new StringBuilder(FORMAT_DATE + "(");
      for (int i = 1; i <= numberOfArguments; i++) {
        changed.append(arguments.get(numberOfArguments - i));
        if (i != numberOfArguments) {
          changed.append(",");
        }
      }
      changed.append(")");
      rectified = rectified.replace(original, changed.toString());
    }
    return rectified;
  }

  private static String rectifyBitwiseOperator(String s) {
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = "(\\d*)\\^(\\d*)";
    String replacement = "POW\\($1,$2\\)";
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    return rectified;
  }

  private static String rectifyToDate(String s){
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = TO_DATE;
    String replacement = TO_DATE;
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    int numberOfArguments = 2;
    int numberOfOccurences = countPattern(rectified, Pattern.compile(TO_DATE, Pattern.CASE_INSENSITIVE));
    for(int count = 1; count <= numberOfOccurences ;count++) {
      List<String> arguments = getArguments(rectified, TO_DATE, numberOfArguments);
      if (arguments.isEmpty()) {
        break;
      }
      arguments.set(1, checkTimeFormat(arguments.get(1)));
      List<Integer> boundary = getFunctionBoundaries(rectified, TO_DATE);
      String original = rectified.substring(boundary.get(0),boundary.get(1) + 1);
      StringBuilder changed = new StringBuilder(PARSE_DATE + "(");
      for (int i = 1; i <= numberOfArguments; i++) {
        changed.append(arguments.get(numberOfArguments - i));
        if (i != numberOfArguments) {
          changed.append(",");
        }
      }
      changed.append(")");
      rectified = rectified.replace(original, changed.toString());
    }
    return rectified;
  }

  private static String rectifyToTimestamp(String s){
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = TO_TIMESTAMP;
    String replacement = TO_TIMESTAMP;
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    int numberOfArguments = 2;
    int numberOfOccurences = countPattern(rectified, Pattern.compile(TO_TIMESTAMP, Pattern.CASE_INSENSITIVE));
    for(int count = 1; count <= numberOfOccurences ;count++) {
      List<String> arguments = getArguments(rectified, TO_TIMESTAMP, numberOfArguments);
      if (arguments.isEmpty()) {
        break;
      }
      arguments.set(1, checkTimeFormat(arguments.get(1)));
      List<Integer> boundary = getFunctionBoundaries(rectified, TO_TIMESTAMP);
      String original = rectified.substring(boundary.get(0),boundary.get(1) + 1);
      StringBuilder changed = new StringBuilder(PARSE_TIMESTAMP + "(");
      for (int i = 1; i <= numberOfArguments; i++) {
        changed.append(arguments.get(numberOfArguments - i));
        if (i != numberOfArguments) {
          changed.append(",");
        }
      }
      changed.append(")");
      rectified = rectified.replace(original, changed.toString());
    }
    return rectified;
  }

  private static String rectifyDayOfWeek(String s){
    if(StringUtils.isEmpty(s)){
      return s;
    }
    String regexTarget = DAYOFWEEK;
    String replacement = VERTICA_SUB;
    String rectified = s.replaceAll(CASE_INSENSITIVE + regexTarget, replacement);
    int numberOfArguments = 1;
    int numberOfOccurences = countPattern(rectified, Pattern.compile(VERTICA_SUB, Pattern.CASE_INSENSITIVE));
    for(int count = 1; count <= numberOfOccurences ;count++) {
      List<String> arguments = getArguments(rectified, VERTICA_SUB, numberOfArguments);
      if (arguments.isEmpty()) {
        break;
      }
      List<Integer> boundary = getFunctionBoundaries(rectified, VERTICA_SUB);
      String original = rectified.substring(boundary.get(0),boundary.get(1) + 1);
      StringBuilder changed = new StringBuilder(EXTRACT + "(" + DAYOFWEEK + " from ");
      for (int i = 1; i <= numberOfArguments; i++) {
        changed.append(arguments.get(numberOfArguments - i));
        if (i != numberOfArguments) {
          changed.append(",");
        }
      }
      changed.append(")");
      rectified = rectified.replace(original, changed.toString());
    }
    return rectified;
  }

  @SneakyThrows
  private static List<String> getArguments(String s, String text, int numberOfArguments){
    if(!s.contains(text)) {
      return Collections.emptyList();
    }
    int start = s.toLowerCase().indexOf(text);
    int startParen = s.toLowerCase().indexOf("(", start);
    int nextComma;
    int nextPara;
    int prevComma = startParen;
    int endParen = findClosingParen(s, startParen);
    List<String> arguments = new ArrayList<String>();
    for(int i = 1; i<= numberOfArguments ;i++){

      nextComma = i!=numberOfArguments ? s.indexOf(",",prevComma+1) : endParen;
      nextPara = s.indexOf("(",prevComma+1) != -1 ? s.indexOf("(",prevComma+1):s.length();
      if(nextPara > nextComma){
        arguments.add(s.substring(prevComma+1,nextComma));
      } else {
        int closingPara = findClosingParen(s, nextPara);
        while (closingPara < nextComma){
          int para = (s.indexOf("(", closingPara) < nextComma) && (s.indexOf("(", closingPara) != -1) ? s.indexOf("(", closingPara) : -1;
          closingPara = para != -1 ? findClosingParen(s, para): nextComma;
        }
        nextComma = i!=numberOfArguments ? s.indexOf(",",closingPara) : endParen;
        arguments.add(s.substring(prevComma+1,nextComma));
      }
      prevComma =nextComma;
    }
    return arguments;


  }

  @SneakyThrows
  private static List<Integer> getFunctionBoundaries(String s, String text){
    int start = s.toLowerCase().indexOf(text);
    int startParen = s.toLowerCase().indexOf("(", start);
    int endParen = findClosingParen(s, startParen);
    List<Integer> arguments = new ArrayList<Integer>();
    arguments.add(start);
    arguments.add(endParen);
    return arguments;


  }

  @SneakyThrows
  private static int findClosingParen(String s, int openPos) {
    int closePos = openPos;
    int counter = 1;
    while (counter > 0) {
      char c = s.charAt(++closePos);
      if (c == '(') {
        counter++;
      }
      else if (c == ')') {
        counter--;
      }
    }
    return closePos;
  }

  private static int countPattern(String references, Pattern referencePattern) {
    Matcher matcher = referencePattern.matcher(references);
    return Stream.iterate(0, i -> i + 1)
        .filter(i -> !matcher.find())
        .findFirst()
        .get();
  }

  private static String checkTimeFormat(String s) {
    if(StringUtils.isEmpty(s)){
      return s;
    }
    for (String key : timeFormatMapping.keySet()) {
      s=s.replaceAll(CASE_INSENSITIVE + key, timeFormatMapping.get(key));
    }
    return s;
  }

}
