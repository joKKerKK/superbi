package com.flipkart.fdp.superbi.web.util;

import com.flipkart.fdp.superbi.utils.SuperbiUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by akshaya.sharma on 18/05/20
 */
@Slf4j
public class SuperbiUtilTest {

  private String generateHostsString(String prefix, int count, int port) {
    String result = prefix + 0;

    result = port > 0 ? result + ":" + port : result;

    for (int i = 1; i < count; i++) {
      result = result + "," + prefix + i;
      result = port > 0 ? result + ":" + port : result;
    }
    return result;

  }

  @Test
  public void testJDBCUrlWithOneHostNoProperty() {
    String hostString = generateHostsString("host", 1, 0);
    String url = "jdbc:mysql:loadbalance://" + hostString + "/hydra_new";

    String randomiseUrl = SuperbiUtil.randomiseJDBCUrl(url);
    Assert.assertTrue(areUrlsEqual(url, randomiseUrl));
  }

  @Test
  public void testJDBCUrlWithOneHostAndWithProperty() {
    String hostString = generateHostsString("host", 1, 0);
    String url = "jdbc:mysql:loadbalance://" + hostString + "/hydra_new?a=b&c=d";

    String randomiseUrl = SuperbiUtil.randomiseJDBCUrl(url);
    Assert.assertTrue(areUrlsEqual(url, randomiseUrl));
  }

  @Test
  public void testJDBCUrlWithNoPropertyAndNoPorts() {
    String hostString = generateHostsString("host", 4, 0);
    String url = "jdbc:mysql:loadbalance://" + hostString + "/hydra_new";

    String randomiseUrl = SuperbiUtil.randomiseJDBCUrl(url);
    Assert.assertTrue(areUrlsEqual(url, randomiseUrl));
  }

  @Test
  public void testJDBCUrlWithNoPropertyAndWithPorts() {
    String hostString = generateHostsString("host", 4, 3306);
    String url = "jdbc:mysql:loadbalance://" + hostString + "/hydra_new";

    String randomiseUrl = SuperbiUtil.randomiseJDBCUrl(url);
    Assert.assertTrue(areUrlsEqual(url, randomiseUrl));
  }

  @Test
  public void testJDBCUrlWithPropertyAndNoPorts() {
    String hostString = generateHostsString("host", 4, 0);
    String url = "jdbc:mysql:loadbalance://" + hostString + "/hydra_new?a=b&c=d";

    String randomiseUrl = SuperbiUtil.randomiseJDBCUrl(url);
    Assert.assertTrue(areUrlsEqual(url, randomiseUrl));
  }

  @Test
  public void testJDBCUrlWithPropertyAndWithPorts() {
    String hostString = generateHostsString("host", 4, 3306);
    String url = "jdbc:mysql:loadbalance://" + hostString + "/hydra_new?a=b&c=d";

    String randomiseUrl = SuperbiUtil.randomiseJDBCUrl(url);
    Assert.assertTrue(areUrlsEqual(url, randomiseUrl));
  }

  @Test
  public void testSanityOfTest() {
    String u1 = "a://h1,h2/c?d=e";
    String u2 = "a://h1,h2/c?d=e";
    String u3 = "a://h2,h1/c?d=e";
    Assert.assertTrue(areUrlsEqual(u1, u2) && areUrlsEqual(u1, u3));

    u1 = "a://h1:3306,h2:3306/c?d=e";
    u2 = "a://h1:3306,h2:3306/c?d=e";
    u3 = "a://h2:3306,h1:3306/c?d=e";

    Assert.assertTrue(areUrlsEqual(u1, u2) && areUrlsEqual(u1, u3));

    u1 = "a://h1:3306,h2:3306/c?d=e";
    u2 = "a://h1:3306,h2/c";
    u3 = "a://h2,h1:3306/c?d=e";

    Assert.assertFalse(areUrlsEqual(u1, u2));
    Assert.assertFalse(areUrlsEqual(u1, u3));
    Assert.assertFalse(areUrlsEqual(u3, u2));
  }

  private boolean areUrlsEqual(String url1, String url2) {
    log.info(url1);
    log.info(url2);
    boolean areOfSameLength = url1.length() == url2.length();

    if(!areOfSameLength) {
      return false;
    }
    String scheme1 = url1.split("://")[0];
    String scheme2 = url2.split("://")[0];

    String path1 = url1.substring(url1.lastIndexOf("/"));
    String path2 = url1.substring(url1.lastIndexOf("/"));

    if(scheme1.equals(scheme2) && path1.equals(path2)) {
      String hostStr1 = url1.substring(url1.indexOf("://") + 3, url1.lastIndexOf("/"));
      String hostStr2 = url2.substring(url2.indexOf("://") + 3, url2.lastIndexOf("/"));

      if(hostStr1.length() != hostStr2.length()) {
        return false;
      }

      String[] allHosts = hostStr1.split(",");
      for (String host1: allHosts) {
        if(!hostStr2.contains(host1)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
