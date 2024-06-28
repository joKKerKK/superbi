package com.flipkart.fdp.superbi.utils;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 18/05/20
 */
@Slf4j
public class SuperbiUtil {
  /*
     Group1 = jdbc:mysql:loadbalance
     Group2 = host[:port]
     Group3 = dbname?properties
   */
  private final static String JDBC_URL_REGEX = "^jdbc:(.+?)(?=:\\/\\/)\\:\\/\\/(.+?)(?=\\/)\\/(.*)$";
  private final static Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);

  /**
   * JDBCUrl pattern
   * jdbc:mysql:loadbalance://[host1][:port],[host2][:port][,[host3][:port]]...[/[database]] »
   * [?propertyName1=propertyValue1[&propertyName2=propertyValue2]...]
   * This method randomises the hosts in the JDBCUrl to take benefit of the load balancing.
   * https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-usagenotes-j2ee-concepts-managing-load-balanced-connections.html
   * @param jdbcUrl
   * @return radomisedUrl
   */
  public static String randomiseJDBCUrl(String jdbcUrl) {
    try {
      // Now create matcher object.
      Matcher m = JDBC_URL_PATTERN.matcher(jdbcUrl);
      if (!m.find( ) && m.groupCount() != 3) {
        throw new RuntimeException("Could not parse JDBCUrl");
      }

      String scheme = m.group(1);
      String hosts = m.group(2);
      String path = m.group(3);

      String randomisedUrl = buildJDBCUrl(scheme, hosts, path);

      log.warn("Randomise JDBC url: {} to {}", jdbcUrl, randomisedUrl);

      return randomisedUrl;
    }catch (Exception ex) {
      log.warn("Could not randomise JDBC url: {}", jdbcUrl, ex);
    }
    return jdbcUrl;
  }

  private static String buildJDBCUrl(String scheme, String hosts, String path) {
    String randoimsedHosts = randomiseString(hosts, ",");
    return MessageFormat.format("jdbc:{0}://{1}/{2}", new String[] {scheme, randoimsedHosts, path} );
  }

  private static String randomiseString(String originalString, String separator) {
    if(StringUtils.isBlank(originalString) || StringUtils.isBlank(separator)) {
      return originalString;
    }

    String[] parts = StringUtils.split(originalString, separator);
    String[] shuffledParts = shuffleArray(parts);

    return StringUtils.join(shuffledParts, separator);
  }

  private static<T> T[] shuffleArray(T[] array) {
    if(array == null) {
      return array;
    }
    Random rand = new Random();

    T[] shuffled = Arrays.copyOf(array, array.length);

    for (int i = 0; i < shuffled.length; i++) {
      int randomPosition = rand.nextInt(shuffled.length);
      T temp = shuffled[i];
      shuffled[i] = shuffled[randomPosition];
      shuffled[randomPosition] = temp;
    }

    return shuffled;
  }
}
