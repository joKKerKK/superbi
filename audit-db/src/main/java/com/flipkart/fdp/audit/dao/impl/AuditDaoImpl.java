package com.flipkart.fdp.audit.dao.impl;

import com.flipkart.fdp.audit.dao.AuditDao;
import com.flipkart.fdp.audit.entities.AuditInfo;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import javax.persistence.EntityManager;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class AuditDaoImpl extends GenericDAOImpl<AuditInfo, Long> implements AuditDao {
  @Inject
  public AuditDaoImpl(@Named("AUDIT") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

}
