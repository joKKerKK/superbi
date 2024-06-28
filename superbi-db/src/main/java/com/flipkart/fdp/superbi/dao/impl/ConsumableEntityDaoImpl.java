package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.entities.ConsumableEntity;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 08/07/19
 */

public abstract class ConsumableEntityDaoImpl<E extends ConsumableEntity> extends
    GenericDAOImpl<E, Long> {
  public ConsumableEntityDaoImpl(GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

}
