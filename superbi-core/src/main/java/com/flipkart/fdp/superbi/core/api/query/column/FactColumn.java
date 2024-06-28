package com.flipkart.fdp.superbi.core.api.query.column;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.fdp.superbi.core.api.query.PanelEntry;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.GroupByType.*;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.FilterType.*;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.SelectType.*;

import com.flipkart.fdp.superbi.core.api.query.exception.ColumnMissingException;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by rajesh.kannan on 09/01/15.
 */
@Slf4j
public abstract class FactColumn {

    String factName;
    protected int index;

    @JsonIgnore
    private PanelEntry entry;

    protected static String URI_LOV_GENERIC_FORMAT = "/cosmos/v2/lov/%s/%s";
    protected static String URI_LOV_DIM_LEVEL_FORMAT = "/cosmos/v2/lov/%s/%s/%s";

    protected static String URI_COL_GENERIC_FORMAT = "/cosmos/v2/lov/factcolumn/%s";
    protected static String URI_COL_DIM_LEVEL_FORMAT = "/cosmos/v2/lov/dim/%s/%s";

    static ImmutableSet<String> rangeTypes = ImmutableSet.<String>builder()
            .add(String.valueOf(DATE_HISTOGRAM))
            .add(String.valueOf(FILTER_DATE_RANGE))
            .add(String.valueOf(METRIC_BUCKETING)).build();

    static ImmutableSet<String> genericDimTypes = ImmutableSet.<String>builder()
            .add(String.valueOf(GROUP_BY_GENERIC))
            .add(String.valueOf(SELECT_AGGREGATION))
            .add(String.valueOf(SELECT_GENERIC))
            .add(String.valueOf(FILTER_GENERIC))
            .addAll(rangeTypes)
            .build();

    static ImmutableSet<String> dimLevelTypes = ImmutableSet.<String>builder()
            .add(String.valueOf(GROUP_BY_DIMENSION_LEVEL))
            .add(String.valueOf(SELECT_DIMENSION_LEVEL))
            .add(String.valueOf(FILTER_DIMENSION_LEVEL))
            .add(String.valueOf(SELECT_AGGREGATION))
            .build();

    static ImmutableSet<String> nonHieDimTypes = ImmutableSet.<String>builder()
            .add(String.valueOf(FILTER_DIMENSION_COLUMN))
            .add(String.valueOf(SELECT_DIMENSION_COLUMN))
            .add(String.valueOf(GROUP_BY_DIMENSION_COLUMN)).build();

    static ImmutableSet<String> exprTypes = ImmutableSet.<String>builder()
            .add(String.valueOf(SELECT_EXPRESSION))
            .add(String.valueOf(SELECT_AGGREGATED_EXPRESSION))
            .add(String.valueOf(FILTER_EXPRESSION))
            .add(String.valueOf(GROUP_BY_EXPRESSION))
            .build();


    public FactColumn(String factName) {
        this.factName = factName;
    }

    public <T extends Enum> String enumOf(Class<T> klass) {

        if(klass.isAssignableFrom(PanelEntry.SelectType.class))
            return PanelEntry.SelectType.SELECT_GENERIC.toString();

        else if(klass.isAssignableFrom(PanelEntry.GroupByType.class))
            return PanelEntry.GroupByType.GROUP_BY_GENERIC.toString();

        else if(klass.isAssignableFrom(PanelEntry.FilterType.class))
            return PanelEntry.FilterType.FILTER_GENERIC.toString();

        throw new IllegalArgumentException("Invalid Enum class: "+ klass);
    }


    public abstract Map<String,Object> getMeta();
    public abstract String getFQName();
    public abstract String getRName();
    public String getEqualsKeyForDiffFact(){return getParamName();}
    public String getEqualsKeyForSameFact(){return getParamName();}

    String getTypeStrForDim()
    {
        if(entry.isDateRangeOrHistogram())
            return "$"+entry.getColumnType();
        return "";
    }

    public String getParamName()
    {
        return getFQName();
    }
    public boolean isLevelAssociated() {
        return false;
    }
    public boolean isDimAssociated() {
        return false;
    }

    public String buildLovURI() {
        return null;
    }

    public String buildColumnsURI() {
        throw new UnsupportedOperationException();
    }

    public PanelEntry getEntry() {
        return entry;
    }

    public static FactColumn valueOf(final String factName, final PanelEntry entry, ColumnFactory.PanelContext context) {

        if("approved_date_key".equals(entry.getColumn().factColumnName)) {
            System.out.println("in logic");
        }
        for (ColumnFactory creator : ColumnFactory.getFactories()) {
            Optional<FactColumn> colOptional = creator.withContext(context).tryCreate(factName, entry);
            if (colOptional.isPresent())
            {
                FactColumn col = colOptional.get();
                col.entry = entry;
                if (entry.isDynamicEntry() &&  col.isDimAssociated()) {
                    String key = col.getEqualsKeyForDiffFact();
                    col.index = context.getCountAndAdd(key, entry);
                }
                return colOptional.get();
            }
        }
        log.error("ColumnMissingException: FactColumn not available for colType: " + entry.getColumnType() + " factName: " + factName + " factColumnName: "
                + entry.getColumn().columnName + " dimensionName: null hierarchyName: null levelName: null");
        throw new ColumnMissingException(entry.getColumnType(), factName, entry.getColumn().columnName, null, null, null);
    }

    /*
     * this method does not update any PanelEntry fields as it is used as a isolated utility method
     */
    public static FactColumn valueOf(final String factName, final PanelEntry.MetaColumn metaColumn, ColumnFactory.PanelContext context) {

        for (ColumnFactory creator : ColumnFactory.getFactories()) {
            Optional<FactColumn> colOptional = creator.withContext(context).tryCreate(factName, "", metaColumn );
            if (colOptional.isPresent())
            {
                return colOptional.get();
            }
        }
        throw new IllegalArgumentException("Unable to create FactColumn with json = \n"+ JsonUtil.toJson(metaColumn));
    }


    //todo this overloaded method is used only for migration as of now, make it robust for the feature DSL to QueryPanel conversion
    public static FactColumn valueOf(final String factName, final String fqName, String alias, PanelEntry.ExprType type, PanelEntry.ColumnCategory category) {

        //todo create schemastore in one place per query panel/ds query and share to all building operations like above.
        ColumnFactory.PanelContext context = new ColumnFactory.PanelContext(factName);

        for (ColumnFactory creator : ColumnFactory.getFactories()) {
            Optional<FactColumn> col = creator.withContext(context).tryCreate(factName, fqName);
            if (col.isPresent())
                return col.get();
        }

        if((category == PanelEntry.ColumnCategory.SELECT || category == PanelEntry.ColumnCategory.FILTER) &&
                couldBeAMissingFactColumnName(factName,fqName))
        {
            throw new IllegalStateException(String.format("Column Name (%s) could be missing from the fact (%s), Please " +
                    "check the DS Query",fqName, factName));
        }

        //As there is no direct way to find whether the given str is expr, default case is used for that

        return new ExprColumn(factName, fqName, alias);

    }

    static boolean couldBeAMissingFactColumnName(String factName, String fqName)
    {
        List<String> wordsToOmit = ImmutableList.<String>builder().add("null").add("distinct").build();
        if(fqName.startsWith(factName + '.'))
            fqName = fqName.substring(factName.length() + 1);

        final String finalFqName = fqName;

        return !(wordsToOmit.stream().anyMatch(word -> finalFqName.toLowerCase().contains(word))) && fqName.matches("(\\w|\\s)+");
    }


    public static FactColumn valueOf(final String factName, final String fqName, PanelEntry.ColumnCategory category) {

        return valueOf(factName, fqName, null, PanelEntry.ExprType.GENERIC, category);
    }
    static Map<String,Object> asMap(Object o)
    {
        return JsonUtil.convertValue(o, Map.class);
    }

    public interface Cascadable
    {
        public String getDimension();
        public String getHierarchy();
        public  String getUptoHierName();
    }
}
