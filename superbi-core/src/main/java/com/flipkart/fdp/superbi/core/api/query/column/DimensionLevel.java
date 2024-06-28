package com.flipkart.fdp.superbi.core.api.query.column;

import com.flipkart.fdp.superbi.core.api.query.PanelEntry;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact;
import com.google.common.base.Optional;
import java.util.Map;

/**
 * Created by rajesh.kannan on 12/01/15.
 */
public class DimensionLevel extends DimMapping implements FactColumn.Cascadable {

    private final Optional<Dimension.Level> dimLevel;
    private final String hierarchyName;

    DimensionLevel(String factName, Fact.DimensionMapping dimensionMapping, String hierarchyName, Optional<Dimension.Level> dimLevel)
    {
        super(factName, dimensionMapping);
        this.hierarchyName = hierarchyName;
        this.dimLevel = dimLevel;

    }

    @Override
    public <T extends Enum> String enumOf(Class<T> klass) {

        if(klass.isAssignableFrom(PanelEntry.SelectType.class))
            return PanelEntry.SelectType.SELECT_DIMENSION_LEVEL.toString();

        else if(klass.isAssignableFrom(PanelEntry.GroupByType.class))
            return PanelEntry.GroupByType.GROUP_BY_DIMENSION_LEVEL.toString();

        else if(klass.isAssignableFrom(PanelEntry.FilterType.class))
            return PanelEntry.FilterType.FILTER_DIMENSION_LEVEL.toString();

        throw new IllegalArgumentException("Invalid Enum class: "+ klass);
    }

    @Override
    public Map<String,Object> getMeta() {

        Map dimMapping =  (Map)super.getMeta();
        dimMapping.put("hierarchyName",hierarchyName);
        if(dimLevel.isPresent())
            dimMapping.putAll(asMap(dimLevel.get()));

        return dimMapping;
    }

    @Override
    public  String getFQName() {
        return getUptoHierName() + "." + dimLevel.get().getName();
    }

    @Override
    public String getRName() {
        return dimLevel.get().getName();
    }

    @Override
    public String buildLovURI() {
        return String.format(URI_LOV_DIM_LEVEL_FORMAT, dimMapping.getDimensionName(), hierarchyName, dimLevel.get().getName());
    }

    @Override
    public  String buildColumnsURI() {
        return String.format(URI_COL_DIM_LEVEL_FORMAT, dimMapping.getDimensionName(), hierarchyName);
    }

    public String getUptoHierName() {
        return String.format("%s.%s.%s", dimMapping.getFactColumnName(), dimMapping.getDimensionName(), hierarchyName);
    }

    @Override
    public String getEqualsKeyForDiffFact() {
        return String.format("%s.%s.%s$%d%s", dimMapping.getDimensionName(), hierarchyName, dimLevel.get().getName(), index, getTypeStrForDim());
    }

    public boolean isLevelAssociated() {
        return true;
    }


    @Override
    public String getDimension() {
        return dimMapping.getDimensionName();
    }

    @Override
    public String getHierarchy() {
        return hierarchyName;
    }

    public boolean isDimAssociated() {
        return true;
    }

}
