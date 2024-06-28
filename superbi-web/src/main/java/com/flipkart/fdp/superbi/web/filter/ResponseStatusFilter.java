package com.flipkart.fdp.superbi.web.filter;

import com.flipkart.fdp.superbi.core.model.DownloadResponse;
import com.flipkart.fdp.superbi.core.model.QueryInfo;
import com.flipkart.fdp.superbi.core.model.ReportDataResponse;
import com.flipkart.fdp.superbi.core.model.TargetDataResponse;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

/**
 * Created by akshaya.sharma on 17/07/19
 */
@Priority(Priorities.USER)
public class ResponseStatusFilter implements ContainerResponseFilter {

  @Override
  public void filter(ContainerRequestContext containerRequestContext,
      ContainerResponseContext containerResponseContext) {
    Object responseEntity = containerResponseContext.getEntity();
    //Check if response entity is not null.
    if (responseEntity == null) {
      return;
    }

    // We only tweak status for ReportDataResponse and TargetDataResponse
    if(responseEntity instanceof ReportDataResponse) {
      handleReportDataResponse((ReportDataResponse) responseEntity, containerResponseContext);
    }else if(responseEntity instanceof TargetDataResponse){
      handleTargetDataResponse((TargetDataResponse) responseEntity, containerResponseContext);
    } else if(responseEntity instanceof DownloadResponse && ((DownloadResponse) responseEntity).isRedirect()){
      containerResponseContext.setStatusInfo(Response.Status.FOUND);
      containerResponseContext.getHeaders().add(HttpHeaders.LOCATION, ((DownloadResponse) responseEntity).getUrl());
    }
  }


  private void handleReportDataResponse(ReportDataResponse reportDataResponse, ContainerResponseContext containerResponseContext) {
    if(reportDataResponse.getQueryCachedResult() != null) {
      // I've data. Lets serve it.
      containerResponseContext.setStatusInfo(Response.Status.OK);
    }else if(QueryInfo.DATA_CALL_TYPE.QUERY_LOCKED.equals(reportDataResponse.getDataCallType())){
      // I do not have data. but report is locked.
      containerResponseContext.setStatusInfo(Response.Status.OK);
    }else {
      containerResponseContext.setStatusInfo(Response.Status.ACCEPTED);
    }
  }

  private void handleTargetDataResponse(TargetDataResponse targetDataResponse, ContainerResponseContext containerResponseContext) {
    List<ReportDataResponse> queryResponses = targetDataResponse.getQueryResponses() != null ? targetDataResponse.getQueryResponses() :
        Collections.EMPTY_LIST;

    if(!queryResponses.isEmpty()) {
      // I've data. Lets serve it.
      containerResponseContext.setStatusInfo(Response.Status.OK);
    }else if(QueryInfo.DATA_CALL_TYPE.QUERY_LOCKED.equals(targetDataResponse.getDataCallType())){
      // I do not have data. but report query is locked. Case : TargetMappings not defined
      containerResponseContext.setStatusInfo(Response.Status.OK);
    }else {
      // I do not have data. and report query is not locked yet
      containerResponseContext.setStatusInfo(Response.Status.ACCEPTED);
    }
  }
}

