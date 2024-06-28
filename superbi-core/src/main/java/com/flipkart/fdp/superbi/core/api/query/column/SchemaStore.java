package com.flipkart.fdp.superbi.core.api.query.column;

import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.DataSource;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import java.util.Map;

public class SchemaStore {

    //Adding fact for debugging purpose
    private Fact fact;

    private Map<String, Fact.Measure> measureMap;

    public final Map<String, Fact.DimensionMapping> mappingsMap;

    private Map<String, Fact.DegenerateDimension> degDimensionMap;

    public SchemaStore(String factName) {

        fact = (Fact) MetaAccessor.get().getEntity(factName, DataSource.Type.fact);

        measureMap = Maps.uniqueIndex(fact.getMeasures(), new Function<Fact.Measure, String>() {
            public String apply(Fact.Measure from) {
                return from.getName();
            }
        });

        mappingsMap = Maps.uniqueIndex(fact.getDimensionsMapping(), new Function<Fact.DimensionMapping, String>() {
            public String apply(Fact.DimensionMapping from) {
                return from.getFactColumnName();
            }
        });

        degDimensionMap =  Maps.uniqueIndex(fact.getDegenerateDimensions(), new Function<Fact.DegenerateDimension, String>() {
            public String apply(Fact.DegenerateDimension from) {
                return from.getName();
            }
        });
    }

    public Fact getFact(String factName) {
        return fact;
    }

    public Optional<Fact.Measure> getMeasure(String measureName) {
        return Optional.fromNullable(measureMap.get(measureName));
    }

    public Optional<Fact.DimensionMapping> getDimMapping(String factColumnName) {
        return Optional.fromNullable(mappingsMap.get(factColumnName));
    }


    public Optional<Fact.DegenerateDimension> getDegDimension(String degDimName) {
        return Optional.fromNullable(degDimensionMap.get(degDimName));
    }

    public Optional<Dimension.Level> getDimensionLevel(String dimensionName, String hierarchyName, String levelName) {
        return MetaAccessor.get().getLevel(dimensionName, hierarchyName, levelName);
    }

}