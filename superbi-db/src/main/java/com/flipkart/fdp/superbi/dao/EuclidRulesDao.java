package com.flipkart.fdp.superbi.dao;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.superbi.entities.EuclidRules;
import com.flipkart.fdp.superbi.entities.EuclidRules.IdClass;
import java.util.List;

/**
 * Created by: mansi.jain on 07/09/23.
 */
public interface EuclidRulesDao extends GenericDAO<EuclidRules, IdClass> {

  List<EuclidRules> getFactRules(String factName);

}
