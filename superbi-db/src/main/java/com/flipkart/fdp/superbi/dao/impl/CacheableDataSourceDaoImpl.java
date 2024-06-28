package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.mmg.cosmos.dao.impl.DataSourceDaoImpl;
import com.flipkart.fdp.mmg.cosmos.entities.DataSource;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

/**
 * Created by: mansi.jain on 28/09/23.
 */
public class CacheableDataSourceDaoImpl extends DataSourceDaoImpl {

  @Inject
  public CacheableDataSourceDaoImpl(@Named("COSMOS")  GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

  @Override
  protected TypedQuery<DataSource> getTypedQuery(PredicateProvider<DataSource> predicateProvider,
      Filter filter) {
    TypedQuery<DataSource> typedQuery = super.getTypedQuery(predicateProvider, filter);
    return typedQuery.setHint("org.hibernate.cacheable", true);
  }
}
