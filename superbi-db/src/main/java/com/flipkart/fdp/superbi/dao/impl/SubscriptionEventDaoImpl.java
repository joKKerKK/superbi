package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.dao.SubscriptionEventDao;
import com.flipkart.fdp.superbi.entities.SubscriptionEvent;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;

public class SubscriptionEventDaoImpl extends GenericDAOImpl<SubscriptionEvent, Long> implements
    SubscriptionEventDao {

  @Inject
  protected SubscriptionEventDaoImpl(
      @Named("HYDRA") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }
}
