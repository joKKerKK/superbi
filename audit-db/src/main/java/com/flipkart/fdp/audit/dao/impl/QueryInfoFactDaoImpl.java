package com.flipkart.fdp.audit.dao.impl;

import com.flipkart.fdp.audit.dao.AuditDao;
import com.flipkart.fdp.audit.dao.QueryInfoFactDao;
import com.flipkart.fdp.audit.entities.AuditInfo;
import com.flipkart.fdp.audit.entities.QueryInfoFact;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;

/**
 * Created by akshaya.sharma on 28/08/19
 */

public class QueryInfoFactDaoImpl extends GenericDAOImpl<QueryInfoFact, Long> implements QueryInfoFactDao {
  @Inject
  public QueryInfoFactDaoImpl(@Named("AUDIT") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }
}