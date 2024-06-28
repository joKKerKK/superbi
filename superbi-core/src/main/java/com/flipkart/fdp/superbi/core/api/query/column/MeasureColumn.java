package com.flipkart.fdp.superbi.core.api.query.column;

import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.Measure;
import java.util.Map;

/**
 * Created by rajesh.kannan on 09/01/15.
 */
public class MeasureColumn extends FactColumn {

    private Measure measure;

    MeasureColumn(String factName, Measure measure)
    {
        super(factName);
        this.measure = measure;
    }

    @Override
    public Map<String,Object> getMeta() {
        return  asMap(measure);
    }

    @Override
    public String getFQName() {
        return factName + "." + measure.getName();
    }

    @Override
    public String buildLovURI() {
        return String.format(URI_LOV_GENERIC_FORMAT, factName, measure.getName());
    }

    @Override
    public String buildColumnsURI() {
        return String.format(URI_COL_GENERIC_FORMAT, factName);
    }


    @Override
    public String getRName() {
        return measure.getName();
    }


}
