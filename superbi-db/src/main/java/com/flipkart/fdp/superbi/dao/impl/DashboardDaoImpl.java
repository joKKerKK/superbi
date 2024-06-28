package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.dao.DashboardDao;
import com.flipkart.fdp.superbi.entities.Dashboard;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;

/**
 * Created by akshaya.sharma on 08/07/19
 */

public class DashboardDaoImpl extends ConsumableEntityDaoImpl<Dashboard> implements DashboardDao {
  @Inject
  public DashboardDaoImpl(@Named("HYDRA") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }
}
