package com.flipkart.fdp.superbi.refresher.dao.elastic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import java.util.Optional;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by akshaya.sharma on 03/04/20
 */
@RunWith(PowerMockRunner.class)
public class ElasticSearchExceptionMapTest {
  final ElasticSearchExceptionMap EXCEPTION_MAP = new ElasticSearchExceptionMap();


  private String getClientExceptionType() {
    Random rand = new Random();
    int size = ElasticSearchExceptionMap.CLIENT_SIDE_EXCEPTIONS.size();
    return ElasticSearchExceptionMap.CLIENT_SIDE_EXCEPTIONS.get(rand.nextInt(size));
  }

  @Test
  public void testExceptionTypes() {
    String exceptionType = getClientExceptionType();
    assertTrue(ClientSideException.class.isAssignableFrom(EXCEPTION_MAP.get(exceptionType)));

    // check lowercase
    exceptionType = exceptionType.toLowerCase();
    assertTrue(ClientSideException.class.isAssignableFrom(EXCEPTION_MAP.get(exceptionType)));

    // Check unknown as ServerSideException
    String someUnknownExceptionType = "SOME_UNKNOWN_EXCEPTION";
    assertTrue(ServerSideException.class.isAssignableFrom(EXCEPTION_MAP.get(someUnknownExceptionType)));
  }

  @Test
  public void testExceptionTypesWithElasticSearchPrefix() {
    Optional<String> exceptionType = ElasticSearchExceptionMap.CLIENT_SIDE_EXCEPTIONS.stream().filter(key -> key.startsWith("ELASTICSEARCH_")).findAny();
    if(exceptionType.isPresent()) {
      String esExceptionType = exceptionType.get();
      assertTrue(ClientSideException.class.isAssignableFrom(EXCEPTION_MAP.get(esExceptionType)));
      // check lowercase
      assertTrue(ClientSideException.class.isAssignableFrom(EXCEPTION_MAP.get(esExceptionType.toLowerCase())));

      // check without "ELASTICSEARCH" prefix
      esExceptionType = esExceptionType.substring("ELASTICSEARCH_".length());
      assertTrue(ClientSideException.class.isAssignableFrom(EXCEPTION_MAP.get(esExceptionType)));
      // check lowercase
      assertTrue(ClientSideException.class.isAssignableFrom(EXCEPTION_MAP.get(esExceptionType.toLowerCase())));
    }

    // Check unknown as ServerSideException
    String someUnknownExceptionType = "ELASTICSEARCH_SOME_UNKNOWN_EXCEPTION";
    assertTrue(ServerSideException.class.isAssignableFrom(EXCEPTION_MAP.get(someUnknownExceptionType)));
  }

  @Test
  public void testBuildingExceptionWithMessage() {
    String message = "sample message";
    String exceptionType = getClientExceptionType();
    RuntimeException clientSideExceptionx = EXCEPTION_MAP.buildException(exceptionType, message);

    assertEquals(ClientSideException.class, clientSideExceptionx.getClass());
    assertEquals(message, clientSideExceptionx.getMessage());

    String someUnknownExceptionType = "ELASTICSEARCH_SOME_UNKNOWN_EXCEPTION";
    RuntimeException serverSideExceptionx = EXCEPTION_MAP.buildException(someUnknownExceptionType, message);

    assertEquals(ServerSideException.class, serverSideExceptionx.getClass());
    assertEquals(message, serverSideExceptionx.getMessage());
  }

  @Test
  public void testBuildingExceptionWithMessageAndThrowable() {
    Throwable throwable = new Throwable();

    String message = "sample message";
    String exceptionType = getClientExceptionType();
    RuntimeException queryExecutionException = EXCEPTION_MAP.buildException(exceptionType, message, throwable);

    assertEquals(message, queryExecutionException.getMessage());
    assertEquals(throwable, queryExecutionException.getCause());
  }


  @Test
  public void testParsingException() {
    Throwable throwable = new Throwable();

    String message = "[super_category] query malformed, no start_object after query name";
    String exceptionType = "parsing_exception";
    RuntimeException queryExecutionException = EXCEPTION_MAP.buildException(exceptionType, message, throwable);

    assertTrue(queryExecutionException.getClass().isAssignableFrom(ClientSideException.class));
    assertEquals(message, queryExecutionException.getMessage());
    assertEquals(throwable, queryExecutionException.getCause());
  }

  @Test
  public void testNullExceptionType() {
    assertTrue(ServerSideException.class.isAssignableFrom(EXCEPTION_MAP.get(null)));
  }
}
