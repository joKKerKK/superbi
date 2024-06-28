package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.mmg.cosmos.dao.impl.FactDaoImpl;
import com.flipkart.fdp.mmg.cosmos.entities.Fact;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

/**
 * Created by: mansi.jain on 28/09/23.
 */
public class CacheableFactDaoImpl extends FactDaoImpl {

  @Inject
  public CacheableFactDaoImpl(@Named("COSMOS") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

  @Override
  protected TypedQuery<Fact> getTypedQuery(PredicateProvider<Fact> predicateProvider,
      Filter filter) {
    TypedQuery<Fact> typedQuery = super.getTypedQuery(predicateProvider, filter);
    return typedQuery.setHint("org.hibernate.cacheable", true);
  }

}
