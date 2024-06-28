package com.flipkart.fdp.superbi.cosmos.dsQuery;

import static com.flipkart.fdp.superbi.dsl.query.factory.CriteriaFactory.AND;
import static com.flipkart.fdp.superbi.dsl.query.factory.CriteriaFactory.IN;
import static com.flipkart.fdp.superbi.dsl.query.factory.DSQueryBuilder.select;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.COL;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.PARAM;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.SEL_COL;

import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import java.util.List;

public class DsQueryBuilder {

  public static final int HIGH_CARDINALITY_LIMIT = 5000;

  public static DSQuery getFor(String factName, String degenerateDimName) {
    List<Fact.DegenerateDimension> degerateDims =
        MetaAccessor.get().getDegeneratesBelongingToTheSameHierarchyAs(factName, degenerateDimName);
    List<Criteria> criteria = Lists.newArrayList();
    for(Fact.DegenerateDimension dimension : degerateDims) {
      String columnName = dimension.getColumnName();
      if(degenerateDimName.equals(dimension.getName())) {
        return criteria.isEmpty()?
            select(SEL_COL(columnName)).from(factName).groupBy(columnName).limit(new Integer(
                HIGH_CARDINALITY_LIMIT +1)).build():
            select(SEL_COL(columnName)).from(factName).where(AND(criteria)).groupBy(columnName).limit(new Integer(
                HIGH_CARDINALITY_LIMIT +1)).build();
      } else {
        criteria.add(
            IN(COL(columnName), PARAM(dimension.getName(), DataType.STRING, true))
        );
      }

    }
    return select(SEL_COL(degenerateDimName)).from(factName).where(AND(criteria)).groupBy(degenerateDimName).limit(new Integer(
        HIGH_CARDINALITY_LIMIT +1)).build();
  }

  public static DSQuery getFor(String dimName, String hierarchyName, String levelName) {
    Optional<Dimension.Hierarchy> hierarchy = MetaAccessor.get().getHierarchy(dimName, hierarchyName);

    if(hierarchy.isPresent()) {
      List<Criteria> criteria = Lists.newArrayList();
      Dimension.Hierarchy hierarchy1 = hierarchy.get();
      for(Dimension.Level level : hierarchy1.getLevels()) {
        String levelColName = level.getColumn().getName();
        if(level.getName().equals(levelName)) {
          return criteria.isEmpty()?
              select(SEL_COL(levelColName)).from(hierarchy.get().getTableName()).groupBy(levelColName).limit(new Integer(
                  HIGH_CARDINALITY_LIMIT +1)).build():
              select(SEL_COL(levelColName)).from(hierarchy.get().getTableName()).where(AND(criteria)).groupBy(levelColName).limit(new Integer(
                  HIGH_CARDINALITY_LIMIT +1)).build();
        } else {
          criteria.add(IN(COL(levelColName), PARAM(level.getName())));
        }
      }
    }

    throw new RuntimeException("Level not found!");
  }

}
