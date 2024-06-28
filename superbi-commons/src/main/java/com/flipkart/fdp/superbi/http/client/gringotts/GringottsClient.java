package com.flipkart.fdp.superbi.http.client.gringotts;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.UrlEscapers;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.entity.ContentType;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

/**
 * Created by akshaya.sharma on 02/05/18
 */
@Slf4j
public class GringottsClient {

  public static final int OK_STATUS = 200;
  private final AsyncHttpClient client;
  public static final String SERVICE_NAME = "GRINGOTTS_SERVICE";
  private static final Gson gson = new Gson();

  public static final String EMPTY_STRING = "";
  public static final String BILLING_APP_ID_LABEL_KEY = "billing_app_id";
  public static final String HYPHEN_DELIMITER = "-";
  public static final int MAX_LABEL_SIZE = 63;

  private static final String HAS_PRIVILEGE_PATH =
          "/permissions/hasPrivilege/${org}/${namespace}/${resourceName}/${resourceType" +
                  "}/${privilegeName}";
  private static final String HAS_NEW_PRIVILEGE_PATH =
          "/permissions/hasNewPrivilege/${org}/${namespace}/${resourceName}/${resourceType" +
                  "}/${privilegeName}";

  private static final String GET_SECURITY_ATTRIBUTE_PATH = "/users/user-attributes/security";

  private static final String GET_BILLING_ORG_PATH = "/billingOrg/user";

  private static final String CHECK_GROUP_FOR_USER = "/auth/user/checkGroup";

  private static final String GET_USER_INFO_PATH = "/namespaces/user";
  private static final String GET_USER_NEW_INFO_PATH = "/namespaces/newuser";



  private final GringottsConfiguration gringottsConfiguration;

  public GringottsClient(AsyncHttpClient client,GringottsConfiguration gringottsConfiguration) {
    this.gringottsConfiguration = gringottsConfiguration;
    this.client = client;
  }

  private String getAbsolutePath(String url) {
    return gringottsConfiguration.getBasePath() + url;
  }

  @SneakyThrows
  private static String urlEncodePathParam(String value) {
    return UrlEscapers.urlPathSegmentEscaper().escape(value);
  }


  @SneakyThrows
  public boolean hasPrivillege(String userName, String org, String namespace, String resourceName,
                               String resourceType, String privilegeName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName));
    Preconditions.checkArgument(StringUtils.isNotBlank(org));
    Preconditions.checkArgument(StringUtils.isNotBlank(namespace));
    Preconditions.checkArgument(StringUtils.isNotBlank(resourceName));
    Preconditions.checkArgument(StringUtils.isNotBlank(resourceType));
    Preconditions.checkArgument(StringUtils.isNotBlank(privilegeName));

    Map<String, String> urlResolveMap = Maps.newHashMap();
    urlResolveMap.put("resourceType", urlEncodePathParam(resourceType));
    urlResolveMap.put("org", urlEncodePathParam(org));
    urlResolveMap.put("namespace", urlEncodePathParam(namespace));
    urlResolveMap.put("resourceName", urlEncodePathParam(resourceName));
    urlResolveMap.put("privilegeName", urlEncodePathParam(privilegeName));

    StrSubstitutor substitutor = new StrSubstitutor(urlResolveMap);
    String url = substitutor.replace(HAS_PRIVILEGE_PATH);

    Request request = buildGetRequest(userName, url);

    org.asynchttpclient.Response response = client.executeRequest(
            request).toCompletableFuture().get();
    return OK_STATUS == response.getStatusCode();

  }

  @SneakyThrows
  public boolean hasNewPrivilege(String userName, String org, String namespace, String resourceName,
                                 String resourceType, String privilegeName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName));
    Preconditions.checkArgument(StringUtils.isNotBlank(org));
    Preconditions.checkArgument(StringUtils.isNotBlank(namespace));
    Preconditions.checkArgument(StringUtils.isNotBlank(resourceName));
    Preconditions.checkArgument(StringUtils.isNotBlank(resourceType));
    Preconditions.checkArgument(StringUtils.isNotBlank(privilegeName));

    Map<String, String> urlResolveMap = Maps.newHashMap();
    urlResolveMap.put("resourceType", urlEncodePathParam(resourceType));
    urlResolveMap.put("org", urlEncodePathParam(org));
    urlResolveMap.put("namespace", urlEncodePathParam(namespace));
    urlResolveMap.put("resourceName", urlEncodePathParam(resourceName));
    urlResolveMap.put("privilegeName", urlEncodePathParam(privilegeName));

    StrSubstitutor substitutor = new StrSubstitutor(urlResolveMap);
    String url = substitutor.replace(HAS_NEW_PRIVILEGE_PATH);

    Request request = buildGetRequest(userName, url);

    org.asynchttpclient.Response response = client.executeRequest(
            request).toCompletableFuture().get();
    return OK_STATUS == response.getStatusCode();
  }

  private Request buildGetRequest(String userName, String url) {
    final GringottsConfiguration gringottsConfiguration = this.gringottsConfiguration;

    return new RequestBuilder("GET").setUrl(getAbsolutePath(url))
            .addHeader("X-Client-Id", gringottsConfiguration.getClientId())
            .addHeader("X-Client-Secret", gringottsConfiguration.getClientSecret())
            .addHeader("context", gringottsConfiguration.getContext())
            .addHeader("x-authenticated-user", userName)
            .build();
  }

  @SneakyThrows
  public Map<String, String> getUserSecurityAttributes(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName));
    Request request = buildGetRequest(userName, GET_SECURITY_ATTRIBUTE_PATH);

    org.asynchttpclient.Response response1 = client.executeRequest(
            request).toCompletableFuture().get();

    if(response1.getStatusCode() != OK_STATUS) {
      throw new RuntimeException("Could not get security attributes for user");
    }

    String responseBody = response1.getResponseBody();
    Map<String, String> userSecurityAttributes = gson.fromJson(responseBody, new TypeToken<HashMap<String, String>>() {
    }.getType());

    return userSecurityAttributes;
  }

  @SneakyThrows
  public List<String> getBillingOrg(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName));
    Request request = buildGetRequest(userName, GET_BILLING_ORG_PATH);

    org.asynchttpclient.Response response1 = client.executeRequest(
            request).toCompletableFuture().get();

    if(response1.getStatusCode() != OK_STATUS) {
      throw new RuntimeException("Could not get security attributes for user");
    }

    String responseBody = response1.getResponseBody();
    List<String> billingOrg = gson.fromJson(responseBody, List.class);

    return billingOrg;
  }

  public static String refactorLabelValue(String labelValue) {
    labelValue = labelValue.toLowerCase().replaceAll("[^a-z0-9-_]", HYPHEN_DELIMITER);
    return labelValue.length() > MAX_LABEL_SIZE ? labelValue.substring(0, MAX_LABEL_SIZE) : labelValue;
  }

  public static String getBillingAppId(String org, String namespace) {
    org = Objects.isNull(org) ? EMPTY_STRING : org;
    namespace = Objects.isNull(namespace) ? EMPTY_STRING : namespace;

    return refactorLabelValue(String.format("%s__%s", org, namespace));
  }

  public static String getBillingAppIdLabelKey() {
    return BILLING_APP_ID_LABEL_KEY;
  }

  @SneakyThrows
  public Map<String, String> getBillingLabels(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName));
    Request request = buildGetRequest(userName, GET_USER_INFO_PATH);

    org.asynchttpclient.Response response1 = client.executeRequest(
            request).toCompletableFuture().get();

    if(response1.getStatusCode() != OK_STATUS) {
      throw new RuntimeException("Could not get org namespace for user");
    }
    String responseBody = response1.getResponseBody();

    List userInfo = gson.fromJson(responseBody, List.class);
    Map<String, String> labels = null;

    if (userInfo.size() > 0) {
      LinkedTreeMap<String, String> orgAndNamespaceInfo = (LinkedTreeMap<String, String>) userInfo.get(
              0);

      String org = refactorLabelValue(orgAndNamespaceInfo.getOrDefault("org",EMPTY_STRING));
      String namespace = refactorLabelValue(orgAndNamespaceInfo.getOrDefault("namespace",EMPTY_STRING));

      if(org.equals("")||namespace.equals(""))
        return labels;

      labels = new HashMap<>();
      labels.put("org", org);
      labels.put("namespace", namespace);
      labels.put(getBillingAppIdLabelKey(), getBillingAppId(org, namespace));

    }

    return labels;
  }

  @SneakyThrows
  public Map<String, String> getNewBillingLabels(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName));
    Request request = buildGetRequest(userName, GET_USER_NEW_INFO_PATH);

    org.asynchttpclient.Response response1 = client.executeRequest(
            request).toCompletableFuture().get();

    if(response1.getStatusCode() != OK_STATUS) {
      throw new RuntimeException("Could not get org namespace for user");
    }
    String responseBody = response1.getResponseBody();

    List userInfo = gson.fromJson(responseBody, List.class);
    Map<String, String> labels = null;

    if (userInfo.size() > 0) {
      LinkedTreeMap<String, String> orgAndNamespaceInfo = (LinkedTreeMap<String, String>) userInfo.get(
              0);

      String org = refactorLabelValue(orgAndNamespaceInfo.getOrDefault("org",EMPTY_STRING));
      String namespace = refactorLabelValue(orgAndNamespaceInfo.getOrDefault("namespace",EMPTY_STRING));

      if(org.equals("")||namespace.equals(""))
        return labels;

      labels = new HashMap<>();
      labels.put("org", org);
      labels.put("namespace", namespace);
      labels.put(getBillingAppIdLabelKey(), getBillingAppId(org, namespace));

    }

    return labels;
  }

  @SneakyThrows
  private Request buildRequest(String method, String userName, String url, Object body) {

    return new RequestBuilder(method).setUrl(getAbsolutePath(url))
            .addHeader("X-Client-Id", gringottsConfiguration.getClientId())
            .addHeader("X-Client-Secret", gringottsConfiguration.getClientSecret())
            .addHeader("context", gringottsConfiguration.getContext())
            .addHeader("Content-type", ContentType.APPLICATION_JSON.toString())
            .addHeader("x-authenticated-user", userName)
            .setBody(gson.toJson(body))
            .build();
  }

  @SneakyThrows
  public boolean isUserPartOfRole(String userName, String roleName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName));
    Preconditions.checkArgument(StringUtils.isNotBlank(roleName));

    List<String> roles = Lists.newArrayList(roleName);
    Request request = buildRequest("POST", userName, CHECK_GROUP_FOR_USER, roles);

    org.asynchttpclient.Response response1 = client.executeRequest(
            request).toCompletableFuture().get();

    if (response1.getStatusCode() != OK_STATUS) {
      throw new RuntimeException("Could not check user role association");
    }

    String responseBody = response1.getResponseBody();
    List<String> matchedRoles = gson.fromJson(responseBody,
            new TypeToken<ArrayList<String>>() {
            }.getType());
    return matchedRoles != null && matchedRoles.contains(roleName);
  }
}