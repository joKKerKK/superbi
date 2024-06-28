package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.dao.EuclidRulesDao;
import com.flipkart.fdp.superbi.entities.EuclidRules;
import com.flipkart.fdp.superbi.entities.EuclidRules.IdClass;
import com.google.inject.name.Named;
import java.util.List;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

/**
 * Created by: mansi.jain on 07/09/23.
 */
public class EuclidRulesDaoImpl extends GenericDAOImpl<EuclidRules, IdClass> implements
    EuclidRulesDao {


  @Inject
  protected EuclidRulesDaoImpl(
      @Named("HYDRA") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

  @Override
  public List<EuclidRules> getFactRules(String factName) {
    return filter(new PredicateProvider<EuclidRules>() {
      @Override
      protected Predicate _getPredicate(CriteriaBuilder criteriaBuilder,
          Root<EuclidRules> root,
          Filter filter) {
        Predicate sameFactNamePredicate = criteriaBuilder.equal(root.get("factName"), factName);
        return criteriaBuilder.and(sameFactNamePredicate);
      }
    }, null);
  }
}
