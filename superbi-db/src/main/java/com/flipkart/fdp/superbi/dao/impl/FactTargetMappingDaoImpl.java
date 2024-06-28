package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.dao.FactTargetMappingDao;
import com.flipkart.fdp.superbi.entities.FactTargetMapping;
import com.flipkart.fdp.superbi.entities.FactTargetMappingId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 21/04/20
 */

public class FactTargetMappingDaoImpl extends GenericDAOImpl<FactTargetMapping,
    FactTargetMappingId> implements FactTargetMappingDao {

  @Inject
  protected FactTargetMappingDaoImpl(
      @Named("HYDRA") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }
}
