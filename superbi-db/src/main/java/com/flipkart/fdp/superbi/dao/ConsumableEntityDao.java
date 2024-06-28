package com.flipkart.fdp.superbi.dao;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.superbi.entities.ConsumableEntity;

/**
 * Created by akshaya.sharma on 08/07/19
 */

public interface ConsumableEntityDao<E extends ConsumableEntity> extends GenericDAO<E, Long> {
}