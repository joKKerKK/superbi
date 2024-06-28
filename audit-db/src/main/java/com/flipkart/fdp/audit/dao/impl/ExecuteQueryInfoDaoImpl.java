package com.flipkart.fdp.audit.dao.impl;

import com.flipkart.fdp.audit.dao.ExecuteQueryInfoDao;
import com.flipkart.fdp.audit.entities.ExecutorQueryInfoLog;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;

/**
 * Created by akshaya.sharma on 28/08/19
 */

public class ExecuteQueryInfoDaoImpl extends GenericDAOImpl<ExecutorQueryInfoLog, String>
    implements ExecuteQueryInfoDao {
  @Inject
  public ExecuteQueryInfoDaoImpl(
      @Named("AUDIT") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

}