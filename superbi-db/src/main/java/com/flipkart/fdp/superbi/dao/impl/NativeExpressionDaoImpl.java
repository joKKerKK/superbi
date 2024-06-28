package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.dao.NativeExpressionDao;
import com.flipkart.fdp.superbi.entities.NativeExpression;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang3.StringUtils;

public class NativeExpressionDaoImpl extends GenericDAOImpl<NativeExpression, NativeExpression
    .IdClass> implements NativeExpressionDao {

  @Inject
  protected NativeExpressionDaoImpl(
      @Named("HYDRA") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

  @Override
  public String getNativeExpression(String nativeExpressionName, String factName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(factName));
    Preconditions.checkArgument(StringUtils.isNotBlank(nativeExpressionName));

    NativeExpression nativeExpression = filterOne(new PredicateProvider<NativeExpression>() {
      @Override
      protected Predicate _getPredicate(CriteriaBuilder criteriaBuilder,
                                        Root<NativeExpression> root,
                                        Filter filter) {
        Predicate sameFactNamePredicate = criteriaBuilder.equal(root.get("factName"), factName);
        Predicate sameNativeExpressionNamePredicate = criteriaBuilder.equal(root.get(
            "nativeExpressionName"), nativeExpressionName);
        Predicate sameIsDisabledPredicate = criteriaBuilder.equal(root.get(
            "disabled"), false);


        return criteriaBuilder.and(sameFactNamePredicate, sameNativeExpressionNamePredicate, sameIsDisabledPredicate);
      }
    }, null);
    return nativeExpression.getNativeExpressionLogic();
  }
}
