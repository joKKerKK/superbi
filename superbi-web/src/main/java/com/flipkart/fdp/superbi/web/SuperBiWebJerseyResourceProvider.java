package com.flipkart.fdp.superbi.web;

import com.flipkart.fdp.superbi.web.filter.AuthenticationFilter;
import com.flipkart.fdp.superbi.web.filter.RequestAuditFilter;
import com.flipkart.fdp.superbi.web.filter.RequestCleanupFilter;
import com.flipkart.fdp.superbi.web.filter.ResponseStatusFilter;
import com.flipkart.fdp.superbi.web.resources.DataResource;
import com.flipkart.fdp.superbi.web.resources.HealthCheck;
import com.flipkart.fdp.superbi.web.resources.QueryResource;
import com.flipkart.fdp.superbi.web.resources.ReportResource;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * Created by : waghmode.tayappa Date : Jun 20, 2019
 */
public class SuperBiWebJerseyResourceProvider {

  public List<Class> getJerseyResources() {

    return Lists.<Class>newArrayList(HealthCheck.class, ReportResource.class, DataResource.class,
        QueryResource.class,
        RequestCleanupFilter.class, AuthenticationFilter.class, ResponseStatusFilter.class,
        RequestAuditFilter.class);
  }
}
