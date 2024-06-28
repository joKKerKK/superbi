package com.flipkart.fdp.superbi.core.api.query.column;

import com.flipkart.fdp.superbi.core.api.query.PanelEntry;
import com.flipkart.fdp.superbi.core.api.query.QueryPanel;
import com.flipkart.fdp.superbi.core.api.query.exception.ColumnMissingException;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import java.util.Arrays;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by rajesh.kannan on 12/01/15.
 */
@Slf4j
public abstract class ColumnFactory<T extends FactColumn> {


  public final static char SEPARATOR = '.';

  PanelContext context;

  // use new schema store for every querypanel/ dsquery action
  public ColumnFactory<T> withContext(PanelContext schemaStore) {
    this.context = schemaStore;
    return this;
  }

  private static boolean containsOrEmpty(Set<String> set, String strToFind) {
    return com.google.common.base.Strings.isNullOrEmpty(strToFind) || set.contains(strToFind);
  }

  Optional<T> tryCreate(String factName, PanelEntry entry) {

    return tryCreate(factName, entry.getColumnType(), entry.getColumn());
  }

  Optional<T> tryCreate(String factName, String colType, PanelEntry.MetaColumn metaColumn) {
    return Optional.absent();
  }

  Optional<T> tryCreate(String factName, String fqName) {
    if (getNoOfTokens().isPresent()) {
      fqName = fqName.startsWith(factName + SEPARATOR) ? fqName : (factName + SEPARATOR) + fqName;
      return tokenizeAndGetColumn(fqName, getNoOfTokens().get());
    }
    return Optional.absent();
  }

  Optional<Integer> getNoOfTokens() {
    return Optional.absent();
  }


  Optional<T> getColumn(String... tokens) {
    return Optional.absent();
  }

  Optional<T> tokenizeAndGetColumn(String fqName, int length) {
    String[] tokens = StringUtils.split(fqName, SEPARATOR);
    if (tokens.length > 0 && tokens[0].startsWith("f_")) {
      tokens = Arrays.copyOfRange(tokens, 1, tokens.length);
    }
    return tokens.length == length ? getColumn(tokens) : Optional.<T>absent();
  }


  public static ImmutableList<ColumnFactory> getFactories() {
    return ImmutableList.<ColumnFactory>builder().add(

        new ColumnFactory<MeasureColumn>() {
          @Override
          public Optional<MeasureColumn> tryCreate(String factName, String colType,
              PanelEntry.MetaColumn metaColumn) {
            if (containsOrEmpty(FactColumn.genericDimTypes, colType)) {
              return getColumn(factName, metaColumn.name);
            }
            return Optional.absent();
          }

          @Override
          Optional<Integer> getNoOfTokens() {
            return Optional.of(2);
          }

          Optional<MeasureColumn> getColumn(String... tokens) {

            String factName = tokens[0];
            String measureName = tokens[1];

            Optional<Fact.Measure> measure = context.schemaStore.getMeasure(measureName);
            return measure.isPresent() ? Optional.of(
                new MeasureColumn(factName, measure.get())) : Optional.<MeasureColumn>absent();
          }
        },


        new ColumnFactory<DegDimension>() {
          @Override
          public Optional<DegDimension> tryCreate(String factName, String colType,
              PanelEntry.MetaColumn metaColumn) {

            Optional<DegDimension> result = Optional.absent();
            if (containsOrEmpty(FactColumn.genericDimTypes, colType)) {
              result = getColumn(factName, metaColumn.name);
            }
            return result;
          }

          @Override
          Optional<Integer> getNoOfTokens() {
            return Optional.of(2);
          }

          Optional<DegDimension> getColumn(String... tokens) {
            String factName = tokens[0];
            String degDimName = tokens[1];
            Optional<Fact.DegenerateDimension> degDim = context.schemaStore.getDegDimension(
                degDimName);
            return degDim.isPresent() ? Optional.of(
                new DegDimension(factName, degDim.get())) : Optional.<DegDimension>absent();
          }
        }

        ,


        new ColumnFactory<DimMapping>()

        {
          @Override
          public Optional<DimMapping> tryCreate(String factName, String colType,
              PanelEntry.MetaColumn metaColumn) {

            Optional<DimMapping> result = Optional.absent();
            if (containsOrEmpty(FactColumn.genericDimTypes, colType) &&
                (com.google.common.base.Strings.isNullOrEmpty(metaColumn.hierarchyName))) {
              result = getColumn(factName, metaColumn.factColumnName);
            }
            return result;
          }

          @Override
          Optional<Integer> getNoOfTokens() {
            return Optional.of(2);
          }

          @Override
          Optional<DimMapping> getColumn(String... tokens) {
            String factName = tokens[0];
            final String factColumnName = tokens[1];
            Optional<Fact.DimensionMapping> filtered = context.schemaStore.getDimMapping(
                factColumnName);
            return filtered.isPresent() ? Optional.of(
                new DimMapping(factName, filtered.get())) : Optional.<DimMapping>absent();
          }
        }

        ,


        new ColumnFactory<DimensionLevel>()

        {
          @Override
          public Optional<DimensionLevel> tryCreate(String factName, String colType,
              PanelEntry.MetaColumn metaColumn) {

            Optional<DimensionLevel> result = Optional.absent();
            if (containsOrEmpty(FactColumn.dimLevelTypes, colType)) {
              result = getColumn(factName, metaColumn.factColumnName, metaColumn.dimensionName,
                  metaColumn.hierarchyName, metaColumn.name, colType);
            }
            return result;
          }

          @Override
          Optional<Integer> getNoOfTokens() {
            return Optional.of(5);
          }

          @Override
          Optional<DimensionLevel> getColumn(String... tokens) {
            String factName = tokens[0];
            String factColumnName = tokens[1];
            String dimensionName = tokens[2];
            String hierarchyName = tokens[3];
            String levelName = tokens[4];
            String colType = tokens[5];

            boolean isDimensionLevel = !(com.google.common.base.Strings.isNullOrEmpty(
                dimensionName) ||
                com.google.common.base.Strings.isNullOrEmpty(hierarchyName) ||
                com.google.common.base.Strings.isNullOrEmpty(levelName));

            if (isDimensionLevel) {
              Optional<Dimension.Level> dimLevel = context.schemaStore.getDimensionLevel(
                  dimensionName, hierarchyName, levelName);
              if (dimLevel.isPresent()) {
                Optional<Fact.DimensionMapping> dimMapping = context.schemaStore.getDimMapping(
                    factColumnName);
                if (!dimMapping.isPresent()) {
                  Fact fact = context.schemaStore.getFact(factName);
                  log.error(
                      "ColumnMissingException: Dimension Mapping not available for colType: " +
                          colType + " factName: " + factName + " factColumnName: " +
                          factColumnName +
                          " dimensionName: " + dimensionName + " hierarchyName: " + hierarchyName
                          + " levelName: " + levelName);
                  log.error(
                      "ColumnMissingException: Underlying-fact definition: " + JsonUtil.toJson(
                          fact));
                  log.error(
                      "ColumnMissingException: Underlying-dimension mapping List: " + JsonUtil
                          .toJson(
                          fact.getDimensionsMapping()));
                  log.error(
                      "ColumnMissingException: Underlying-dimension mapping Map: " + JsonUtil
                          .toJson(
                          context.schemaStore.mappingsMap));

                  Set<Fact.DimensionMapping> dimMappingsFromMetaAccessor = MetaAccessor.get().getFactDimensionMappings(factName);
                  log.error(
                      "ColumnMissingException: Underlying-dimension mapping From MetaAccessor: "
                          + JsonUtil.toJson(
                          dimMappingsFromMetaAccessor));
                  throw new ColumnMissingException(colType, factName, factColumnName, dimensionName,
                      hierarchyName, levelName);
                }
                return (dimMapping.isPresent() ? Optional.of(
                    new DimensionLevel(factName, dimMapping.get(), hierarchyName,
                        dimLevel)) : Optional.absent());
              }
              log.error(
                  "ColumnMissingException: Dimension Level not available for colType: " + colType
                      + " factName: " + factName + " factColumnName: " + factColumnName +
                      " dimensionName: " + dimensionName + " hierarchyName: " + hierarchyName + ""
                      + " levelName: " + levelName);
              throw new ColumnMissingException(colType, factName, factColumnName, dimensionName,
                  hierarchyName, levelName);
            }
            return Optional.absent();
          }
        }

        ,
        new ColumnFactory<ExprColumn>()

        {
          @Override
          public Optional<ExprColumn> tryCreate(String factName, PanelEntry entry) {
            if (FactColumn.exprTypes.contains(entry.getColumnType())) {
              return Optional.of(new ExprColumn(factName, entry.getExpr(), entry.getAlias().get()));
            }
            return Optional.absent();
          }
        }

    ).build();
  }


  public static class PanelContext {
    /*filter sections Type, factColumn key, occurrences */
    private final Table<String, String, Integer> usedColumns = HashBasedTable.create();
    public final SchemaStore schemaStore;
    public final QueryPanel panel;

    public PanelContext(QueryPanel panel) {
      this.panel = panel;
      schemaStore = new SchemaStore(panel.getFromTable());
    }

    public PanelContext(String factName) {
      schemaStore = new SchemaStore(factName);
      this.panel = null;
    }

    public int getCountAndAdd(String key, PanelEntry entry) {
      String colType = entry.isDateRangeOrHistogram() ? entry.getColumnType() : "OTHERS";
      int oldCount = Optional.fromNullable(usedColumns.get(colType, key)).or(0);
      usedColumns.put(colType, key, oldCount + 1);
      return oldCount;
    }
  }
}
