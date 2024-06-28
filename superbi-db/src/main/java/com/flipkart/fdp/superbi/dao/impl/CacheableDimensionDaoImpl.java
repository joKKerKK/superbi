package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.mmg.cosmos.dao.impl.DimensionDaoImpl;
import com.flipkart.fdp.mmg.cosmos.entities.Dimension;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

/**
 * Created by: mansi.jain on 28/09/23.
 */
public class CacheableDimensionDaoImpl extends DimensionDaoImpl {

  @Inject
  protected CacheableDimensionDaoImpl(@Named("COSMOS") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

  @Override
  protected TypedQuery<Dimension> getTypedQuery(PredicateProvider<Dimension> predicateProvider,
      Filter filter) {
    TypedQuery<Dimension> typedQuery = super.getTypedQuery(predicateProvider, filter);
    return typedQuery.setHint("org.hibernate.cacheable", true);
  }

}
