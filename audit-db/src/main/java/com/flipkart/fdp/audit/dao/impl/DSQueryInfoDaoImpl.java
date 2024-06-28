package com.flipkart.fdp.audit.dao.impl;

import com.flipkart.fdp.audit.dao.DSQueryInfoDao;
import com.flipkart.fdp.audit.entities.DSQueryInfoLog;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;

public class DSQueryInfoDaoImpl extends GenericDAOImpl<DSQueryInfoLog, Long>
    implements DSQueryInfoDao {

  @Inject
  public DSQueryInfoDaoImpl(
      @Named("AUDIT") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }
}
