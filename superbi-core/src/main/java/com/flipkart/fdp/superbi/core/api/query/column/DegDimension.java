package com.flipkart.fdp.superbi.core.api.query.column;

import com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension;
import java.util.Map;

/**
 * Created by rajesh.kannan on 09/01/15.
 */
public class DegDimension extends FactColumn implements FactColumn.Cascadable {

    private DegenerateDimension degenerateDimension;


    private Dimension.Level levelRef;

    DegDimension(String factName, DegenerateDimension degenerateDimension)
    {
        super(factName);
        this.degenerateDimension = degenerateDimension;
        this.levelRef = degenerateDimension.getLevelRef();

    }
    
    @Override
    public Map<String,Object> getMeta() {
        return  asMap(degenerateDimension);
    }


    @Override
    public String getFQName() {
            return factName + "." + degenerateDimension.getName();
    }

    @Override
    public String getRName() {
        return degenerateDimension.getName();
    }

    public boolean isLevelAssociated() {
        return levelRef != null;
    }

    @Override
    public String buildLovURI() {
        return String.format(URI_LOV_GENERIC_FORMAT, factName, degenerateDimension.getName());
    }

    @Override
    public String buildColumnsURI() {
        return String.format(URI_COL_GENERIC_FORMAT, factName);
    }

    @Override
    public String getUptoHierName() {
            return String.format("%s.%s", levelRef.getDimensionName(), levelRef.getHierarchyName());
    }

    @Override
    public String getEqualsKeyForDiffFact() {
        if(isLevelAssociated())
            return String.format("%s.%s.%s$%d%s", levelRef.getDimensionName(), levelRef.getHierarchyName(), levelRef.getName(),index,getTypeStrForDim());
        else return getEqualsKeyForSameFact();
    }

    @Override
    public String getDimension() {
        return levelRef.getDimensionName();
    }

    @Override
    public String getHierarchy() {
        return levelRef.getHierarchyName();
    }

    public boolean isDimAssociated() {
        return isLevelAssociated();
    }

}
