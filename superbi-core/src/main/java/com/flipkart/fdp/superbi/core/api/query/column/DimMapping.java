package com.flipkart.fdp.superbi.core.api.query.column;

import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.util.Map;

/**
 * Created by rajesh.kannan on 12/01/15.
 */
public class DimMapping extends FactColumn {

    final DimensionMapping dimMapping;
    DimMapping(String factName, DimensionMapping dimMapping)
    {
        super(factName);
        this.dimMapping = dimMapping;
    }

    @Override
    public Map<String,Object> getMeta() {
        Map dimMapping =  JsonUtil.convertValue(this.dimMapping, Map.class);
        return dimMapping;
    }

    public DimensionMapping getDimensionMapping() {
        return dimMapping;
    }
    @Override
    public  String getFQName() {
        return factName + "." + dimMapping.getFactColumnName();
    }

    @Override
    public String getRName() {
        return dimMapping.getFactColumnName();
    }

    @Override
    public String buildLovURI() {
        return String.format(URI_LOV_GENERIC_FORMAT, factName, dimMapping.getFactColumnName());
    }

    @Override
    public String buildColumnsURI() {
        return String.format(URI_COL_GENERIC_FORMAT, factName);
    }

    @Override
    public String getEqualsKeyForDiffFact() {
        return String.format("%s$%d%s", dimMapping.getDimensionName(), index, getTypeStrForDim());
    }
    public boolean isDimAssociated() {
        return true;
    }

}
