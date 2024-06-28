package com.flipkart.fdp.superbi.core.service;

import com.flipkart.fdp.dao.common.STOFactory;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.service.EntityService;
import com.flipkart.fdp.superbi.core.api.FactTargetMappingSTO;
import com.flipkart.fdp.superbi.core.sto.FactTargetMappingSTOFactory;
import com.flipkart.fdp.superbi.dao.FactTargetMappingDao;
import com.flipkart.fdp.superbi.entities.FactTargetMapping;
import com.flipkart.fdp.superbi.entities.FactTargetMappingId;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import java.util.List;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

/**
 * Created by akshaya.sharma on 21/04/20
 */

public class FactTargetMappingService extends EntityService<FactTargetMapping, FactTargetMappingId,
    FactTargetMappingSTO, FactTargetMappingDao> {

  @Inject
  protected FactTargetMappingService(FactTargetMappingDao dao, FactTargetMappingSTOFactory factory) {
    super(dao, factory);
  }

  public List<FactTargetMappingSTO> getTargetMappingsForFact(String factName) {
    return filter(new PredicateProvider<FactTargetMapping>() {
      @Override
      protected Predicate _getPredicate(CriteriaBuilder criteriaBuilder,
          Root<FactTargetMapping> root, Filter filter) {
        List<Predicate> predicates = Lists.newArrayList();

        predicates.add(criteriaBuilder.equal(root.get("mappingId").get("factName"), factName));

        return criteriaBuilder.and(predicates.toArray(new Predicate[]{}));
      }
    }, null);
  }

  @Override
  public String getIdFieldName() {
    return "mappingId";
  }

  public List<FactTargetMappingSTO> getTargetMappingsForFactAndTargetFact(String reportFactName, String targetFactName) {
    return filter(new PredicateProvider<FactTargetMapping>() {
      @Override
      protected Predicate _getPredicate(CriteriaBuilder criteriaBuilder,
          Root<FactTargetMapping> root, Filter filter) {
        List<Predicate> predicates = Lists.newArrayList();

        predicates.add(criteriaBuilder.equal(root.get("mappingId").get("factName"), reportFactName));
        predicates.add(criteriaBuilder.equal(root.get("mappingId").get("targetFactName"), targetFactName));

        return criteriaBuilder.and(predicates.toArray(new Predicate[]{}));
      }
    }, null);
  }
}
