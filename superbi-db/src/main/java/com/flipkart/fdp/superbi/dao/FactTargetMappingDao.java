package com.flipkart.fdp.superbi.dao;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.superbi.entities.FactTargetMapping;
import com.flipkart.fdp.superbi.entities.FactTargetMappingId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 21/04/20
 */

public interface FactTargetMappingDao extends GenericDAO<FactTargetMapping, FactTargetMappingId> {
}
