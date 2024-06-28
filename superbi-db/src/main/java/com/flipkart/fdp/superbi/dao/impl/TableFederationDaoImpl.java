package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.dao.TableFederationDao;
import com.flipkart.fdp.superbi.entities.ReportFederation.IdClass;
import com.flipkart.fdp.superbi.entities.TableFederation;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

/**
 * Created by mansi.jain on 04/03/22
 */
public class TableFederationDaoImpl extends GenericDAOImpl<TableFederation, IdClass> implements
    TableFederationDao {

  @Inject
  protected TableFederationDaoImpl(
      @Named("HYDRA") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

  @Override
  public List<TableFederation> getFederation(String fromTable, String storeIdentifier) {
    /*
      In case overriding store identifier is not present filterOne method was throwing error instead of returning empty list
      To avoid that error, switched from filterOne to filter method.
      If the list is empty then federation properties will now be able to populated by enrichFederationProperties method
     */
    return filter(new PredicateProvider<TableFederation>() {
      @Override
      protected Predicate _getPredicate(CriteriaBuilder criteriaBuilder,
          Root<TableFederation> root,
          Filter filter) {
        Predicate sameTableNamePredicate = criteriaBuilder.equal(root.get("tableName"), fromTable);
        Predicate sameOverridingStoreIdentifier = criteriaBuilder.equal(
            root.get("overridingStoreIdentifier"),
            storeIdentifier);

        return criteriaBuilder.and(sameTableNamePredicate, sameOverridingStoreIdentifier);
      }
    }, null);
  }
}
