package com.flipkart.fdp.superbi.core.util;

import static com.flipkart.fdp.superbi.dsl.query.factory.CriteriaFactory.AND;
import static com.flipkart.fdp.superbi.dsl.query.factory.CriteriaFactory.IN;
import static com.flipkart.fdp.superbi.dsl.query.factory.CriteriaFactory.NEQ;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.COL;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.LIT;

import com.flipkart.fdp.mmg.cosmos.entities.DegenerateDimension;
import com.flipkart.fdp.mmg.cosmos.entities.Dimension;
import com.flipkart.fdp.mmg.cosmos.entities.Fact;
import com.flipkart.fdp.mmg.cosmos.entities.FactDimensionMapping;
import com.flipkart.fdp.mmg.cosmos.entities.Hierarchy;
import com.flipkart.fdp.mmg.cosmos.entities.Level;
import com.flipkart.fdp.mmg.cosmos.entities.SourceType;
import com.flipkart.fdp.superbi.core.model.ModifiedDSQuery;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by akshaya.sharma on 17/07/19
 */

/**
 * TODO Old code from HYDRA, needs cleanup
 */
public class AuthorizationUtil {
  public static Map<String, String> getSecurityKeyMap(Fact fact) {
    final Map<String, String> securityColumnMap = Maps.newHashMap();
    for(DegenerateDimension degenerateDimension: fact.getDegenerateDimensions()) {
      securityColumnMap.put(degenerateDimension.getName(), degenerateDimension.getColumn().getName());
    }
    for(FactDimensionMapping mapping :fact.getFactDimensionMappings()) {
      Dimension dimension = mapping.getDimension();
      for(Hierarchy hierarchy : dimension.getHierarchies()) {
        for(Level level : hierarchy.getLevels()) {
          String levelQualifiedName = dimension.getName() + "_" + hierarchy.getName() + "_" + level.getName();
          securityColumnMap.put(levelQualifiedName,
              mapping.getColumn().getName()
                  + "." + dimension.getName() + "." + hierarchy.getName() + "." + level.getName());
        }
      }
    }
        /*for(final SelectColumn col: query.getSelectedColumns()) {
            securityColumnMap.put(col.expression, col.expression.replace(".", "_"));
        }*/
    return securityColumnMap;
  }

  /**
   * Code copied from HYDRA
   * TODO cleanup
   */
  public static ModifiedDSQuery applySecurityAttributesForUser(DSQuery query, final Map<String, String> securityAttributeValues, Fact fact) {
    if(fact.getTable().getSource().getSourceType().equals(SourceType.DRUID)) {
      return new ModifiedDSQuery(query, Maps.newHashMap());
    }
    final DSQuery.Builder builder = new DSQuery.Builder(query);
    final Map<String, String> securityKeyColumnMap = AuthorizationUtil.getSecurityKeyMap(fact);
    final Set<String> securityAttributesToBeApplied = Sets.intersection(securityAttributeValues.keySet(),
        securityKeyColumnMap.keySet());
    final Map<String, String> appliedFilterValues = Maps.newHashMap();
    List<Criteria> securityCriteriaList = Lists.newArrayList();
    com.google.common.base.Optional<Criteria> existingCriteriaOptional = query.getCriteria();
    for(final String securityAttribute : securityAttributesToBeApplied) {
      final List<Exp> allowedValues = Lists.newArrayList();
      final List<Exp> disAllowedValues = Lists.newArrayList();
      final String comaSeparatedValues = securityAttributeValues.get(securityAttribute);
      final ColumnExp colExp = COL(securityKeyColumnMap.get(securityAttribute));
      appliedFilterValues.put(securityAttribute, comaSeparatedValues);
      for(final String value: comaSeparatedValues.split(",")) {
        if(value.charAt(0) == '!') {
          disAllowedValues.add(LIT(value.substring(1, value.length())));
        } else {
          allowedValues.add(LIT(value));
        }
      }
      if(allowedValues.size() > 0) securityCriteriaList.add(IN(colExp, allowedValues));
      for(Exp exp : disAllowedValues) {
        securityCriteriaList.add(NEQ(colExp, exp));
      }
    }
    if(securityCriteriaList.isEmpty()) {
      return new ModifiedDSQuery(query, appliedFilterValues);
    } else {
      Criteria modifiedCriteria = null;
      int securityCriteriaSize = securityCriteriaList.size();
      if(securityCriteriaSize == 1) {
        modifiedCriteria = securityCriteriaList.get(0);
      } else if (securityCriteriaSize == 2) {
        modifiedCriteria = AND(
            securityCriteriaList.get(0),
            securityCriteriaList.get(1)
        );
      } else {
        modifiedCriteria = AND(
            securityCriteriaList.get(0),
            securityCriteriaList.get(1),
            securityCriteriaList.subList(2, securityCriteriaList.size()).toArray(new Criteria[securityCriteriaList.size()-2])
        );
      }
      if(existingCriteriaOptional.isPresent()) {
        modifiedCriteria = AND(
            existingCriteriaOptional.get(),
            modifiedCriteria
        );
      }
      builder.withCriteria(modifiedCriteria);
      builder.build();
    }
    return new ModifiedDSQuery(
        builder.build(),
        appliedFilterValues
    );
  }
}
