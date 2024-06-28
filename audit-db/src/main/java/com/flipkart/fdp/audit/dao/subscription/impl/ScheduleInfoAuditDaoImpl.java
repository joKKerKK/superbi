package com.flipkart.fdp.audit.dao.subscription.impl;

import com.flipkart.fdp.audit.dao.subscription.ScheduleInfoAuditDao;
import com.flipkart.fdp.audit.entities.ScheduleInfoAudit;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;

public class ScheduleInfoAuditDaoImpl extends GenericDAOImpl<ScheduleInfoAudit, Long> implements
    ScheduleInfoAuditDao {

  @Inject
  public ScheduleInfoAuditDaoImpl(
      @Named("AUDIT") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }
}
