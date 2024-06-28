package com.flipkart.fdp.superbi.web.util;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Created by akshaya.sharma on 10/07/19
 */

public class MapUtilTest {

  @Test
  public void castedArrayTest() {
    List<String> list = Lists.newArrayList("a", "b", "c");

    String[] res = new String[list.size()];

    res = list.toArray(res);

    System.out.println(res);


    String[] strings = Arrays.copyOfRange(res, 0, 0);
    System.out.println(strings.length);

  }

  private <V> void test() {

  }
}
