package com.flipkart.fdp.superbi.cosmos.hystrix;

import com.flipkart.fdp.superbi.cosmos.exception.HttpException;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.FluentStringsMap;
import com.ning.http.client.Response;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Future;

/*
* *
 * Created by amruth.s on 29/12/14.
 */
public class HttpInterceptor {

    public enum ContentType {
        JSON {
            @Override
            public String toString() {
                return "application/json";
            }
        },
        SOAPXML {
            @Override
            public String toString() {
                return "application/soap+xml";
            }
        }
    }

    public static class Config {
        public final String serviceGroupName;
        public final int defaultTimeout;

        public Config(String serviceGroupName, int defaultTimeout) {
            this.serviceGroupName = serviceGroupName;
            this.defaultTimeout = defaultTimeout;
        }
    } 
    
    private AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    private Config config;
    

    private HttpInterceptor() {
    }

    public static HttpInterceptor getFor(Config config) {
        HttpInterceptor util = new HttpInterceptor();
        util.config = config;
        return util;
    }
    
    private RemoteCall<String> prepareGetCall(final String url) {
        return new RemoteCall.Builder<String>(config.serviceGroupName)
                .withTimeOut(config.defaultTimeout)
                .around(new ActualCall<String>() {
                    @Override
                    public String workUnit() {
                        try {
                            return validateAndGetResponseBody(asyncHttpClient.prepareGet(url).execute().get());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });        
    }

    private RemoteCall<String> prepareGetCall(final String url, final FluentCaseInsensitiveStringsMap headers) {
        return new RemoteCall.Builder<String>(config.serviceGroupName)
                .withTimeOut(config.defaultTimeout)
                .around(new ActualCall<String>() {
                    @Override
                    public String workUnit() {
                        try {
                            return validateAndGetResponseBody(asyncHttpClient.prepareGet(url).setHeaders(headers).execute().get());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }
    
    private RemoteCall<String> preparePostCall(final String url, final String data, final ContentType contentType) {
        return new RemoteCall.Builder<String>(config.serviceGroupName)
                .withTimeOut(config.defaultTimeout)
                .around(new ActualCall<String>() {
                    @Override
                    public String workUnit() {
                        try {

                            return validateAndGetResponseBody( asyncHttpClient
                                    .preparePost(url)
                                    .setHeader("Content-Type", contentType.toString())
                                    .setHeader("Content-Length", String.valueOf(data.length()))
                                    .setHeader("Host-Address", getHostName())
                                    .setBody(data)
                                    .execute()
                                    .get());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });        
    }

    private RemoteCall<String> preparePostCall(final String url, final String data, final ContentType contentType, final FluentCaseInsensitiveStringsMap headers) {
        return new RemoteCall.Builder<String>(config.serviceGroupName)
            .withTimeOut(config.defaultTimeout)
            .around(new ActualCall<String>() {
                @Override
                public String workUnit() {
                    try {

                        return validateAndGetResponseBody( asyncHttpClient
                            .preparePost(url)
                            .setHeaders(headers)
                            .setHeader("Content-Type", contentType.toString())
                            .setHeader("Content-Length", String.valueOf(data.length()))
                            .setHeader("Host-Address", getHostName())
                            .setBody(data)
                            .execute()
                            .get());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
    }

    private RemoteCall<String> preparePostCall(final String url, final FluentStringsMap parameters, final FluentCaseInsensitiveStringsMap headers) {
        return new RemoteCall.Builder<String>(config.serviceGroupName)
                .withTimeOut(config.defaultTimeout)
                .around(new ActualCall<String>() {
                    @Override
                    public String workUnit() {
                        try {

                            return validateAndGetResponseBody( asyncHttpClient
                                    .preparePost(url)
                                    .setHeaders(headers)
                                    .setParameters(parameters)
                                    .execute()
                                    .get());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private RemoteCall<String> preparePutCall(final String url, final String data, final ContentType contentType) {
        return preparePutCall(url, data, null);
    }

    private RemoteCall<String> preparePutCall(final String url, final String data, final ContentType contentType, final FluentCaseInsensitiveStringsMap headers) {
        return new RemoteCall.Builder<String>(config.serviceGroupName)
            .withTimeOut(config.defaultTimeout)
            .around(new ActualCall<String>() {
                @Override
                public String workUnit() {
                    try {
                        return validateAndGetResponseBody(asyncHttpClient
                            .preparePut(url)
                            .setHeaders(headers)
                            .setHeader("Content-Type", contentType.toString())
                            .setHeader("Content-Length", String.valueOf(data.length()))
                            .setHeader("Host-Address", getHostName())
                            .setBody(data)
                            .execute()
                            .get());

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
            });
    }

    public String sendGet(String url) {
        return prepareGetCall(url)
                .execute();
    }

    public String sendGet(String url, FluentCaseInsensitiveStringsMap headers) {
        return prepareGetCall(url, headers)
                .execute();
    }
    
    public Future<String> sendGetAsync(String url) {
        return prepareGetCall(url)
                .executeAsync();
    }

    public String sendPostAsSOAP(String url, String data) {
            return preparePostCall(url, data, ContentType.SOAPXML)
					.execute();
    }

    public String sendPost(String url, String data) {
            return preparePostCall(url, data, ContentType.JSON)
                    .execute();
    }

    public String sendPost(String url, String data, FluentCaseInsensitiveStringsMap headers) {
        return preparePostCall(url, data, ContentType.JSON, headers)
            .execute();
    }

    //For requests of the form x-www-form-urlencoded
    public String sendPost(String url, FluentStringsMap parameters, FluentCaseInsensitiveStringsMap headers){
        return preparePostCall(url, parameters, headers)
                .execute();
    }

    private String validateAndGetResponseBody(Response response) throws IOException {
        String responseBody = response.getResponseBody();
        if (response.getStatusCode()/100 != 2) {
			throw new HttpException(response.getStatusCode(), responseBody);
		}
        return responseBody;
    }


    public String sendPut(String url, String data, FluentCaseInsensitiveStringsMap headers) {
        return preparePutCall(url, data, ContentType.JSON, headers)
            .execute();
    }


    public String sendPut(String url, String data) {
        return preparePutCall(url, data, ContentType.JSON)
                .execute();
    }
    
    public Future<String> sendPutAsync(String url, String data) {
        return preparePutCall(url, data, ContentType.JSON)
                .executeAsync();
    }

    private static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception ex) {
            return "Unknown";
        }
    }

    public static String getHostAddressName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception ex) {
            return "Unknown";
        }
    }

}