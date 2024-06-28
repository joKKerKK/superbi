package com.flipkart.fdp.superbi.core.api.query;

import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.JS;
import static com.google.common.base.Optional.absent;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.core.api.query.column.FactColumn;
import com.flipkart.fdp.superbi.core.util.DSQueryBuilder;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Created by akshaya.sharma on 09/07/19
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class PanelEntry implements DefaultParamHolder{
  public final static String START_TIMESTAMP = "startTimestamp";
  public final static String END_TIME_STAMP = "endTimestamp";
  public final static String DOWN_SAMPLE = "downsample";
  public final static String DOWN_SAMPLE_UNIT = "downsampleUnit";
  public final static String SERIES_TYPE = "seriesType";
  public final static String OPERATOR = "operator";
  public final static String START = "start";
  public final static String END = "end";
  public final static String STEP = "step";
  public final static String ORDER = "order";
  public final static String FIELD_ISVISIBLE = "isVisible";
  public final static String[] EMPTY_VALUES = new String[0];


  private Optional<String> alias = absent();
  private MetaColumn column = new MetaColumn();

  @JsonProperty
  private boolean isDynamic;

  @JsonProperty
  private boolean isVisible = true;

  private String columnType;
  private String expr;
  private AggregationType aggregator;
  private Map<String,String> dateRange = null;
  private Map<String,Long> bucket;
  private Optional<OrderByExp.Type> orderBy = absent();

  private Predicate.Type operator = Predicate.Type.eq;
  private List<String> values = Lists.newArrayList();
  private Map<String,String[]> params = Maps.newHashMap();
  private Map<String,Object> otherProps = Maps.newHashMap();
  private List<Object> valuesForSeries;

  @JsonIgnore
  private QueryPanel container;

  private List<PanelEntry> similarEntriesInTab = Lists.newArrayList();

  @JsonProperty("nThFractile")
  private Double nthFractile;


  private Map<String,MetaColumn> operands;

  private PanelEntry criteria;

  private ExprType exprType = ExprType.GENERIC;

  public static MetaColumn valueOf(FactColumn col)
  {
    Map<String, Object> values = col.getMeta();
    return values == null ? null : JsonUtil.convertValue(values, MetaColumn.class);
  }

  public void setFactCol(FactColumn factCol)
  {
    column.setFactColumn(factCol);
  }

  public FactColumn getFactCol()
  {
    return column.getFactColumn();
  }

  public ExprType getExprType() {
    return exprType;
  }

  public boolean isDynamicEntry()
  {
    if(EnumUtils.isValidEnum(FilterType.class, columnType) ||
        (EnumUtils.isValidEnum(GroupByType.class, columnType) && !columnType.equals(String.valueOf(GroupByType.METRIC_BUCKETING))))
      return isDynamic() || columnType.equals(String.valueOf(GroupByType.DATE_HISTOGRAM));
    return false;
  }

  public enum ColumnCategory {SELECT,FILTER, GROUPBY}

  public enum ExprType
  {
    GENERIC, NATIVE, IMPORT
  }

  public enum SelectType
  {SELECT_AGGREGATION,SELECT_AGGREGATED_EXPRESSION,SELECT_EXPRESSION,SELECT_GENERIC, SELECT_DIMENSION_COLUMN,SELECT_DIMENSION_LEVEL}

  public enum GroupByType
  {METRIC_BUCKETING,GROUP_BY_DIMENSION_LEVEL, GROUP_BY_GENERIC, DATE_HISTOGRAM,GROUP_BY_DIMENSION_COLUMN,
    GROUP_BY_EXPRESSION
  }

  public enum FilterType
  {
    FILTER_GENERIC, FILTER_DIMENSION_LEVEL, FILTER_DATE_RANGE, FILTER_DIMENSION_COLUMN, FILTER_EXPRESSION
  }

  public void putDefaultParam(String name, String[] values)
  {
    params.put(name, values);
  }

  @Override
  public Map<String, String[]> getDefaultParams() {
    return params;
  }

  @Override
  public Map<String, String[]> evalAndGetDefaultParams() {
    Map<String,String[]> evaluatedParams = Maps.newHashMap(params);

    if(isDateRangeOrHistogram())
    {
      for(String paramName : params.keySet())
        if(paramName.endsWith("."+START_TIMESTAMP) || paramName.endsWith("."+END_TIME_STAMP))
          evaluatedParams.put(paramName, eval(params.get(paramName),column.type()));
    }
    return evaluatedParams;
  }

  @Override
  public void clearDefaultParams() {
    params.clear();
  }

  private static String[] eval(String expr[], DataType dataType)
  {
    //todo this fails ds parse as int and but js engine generates double
    Map<String,String[]> emptyParams = Maps.newHashMap();
    return ArrayUtils.isEmpty(expr)? expr : DSQueryBuilder.toArr(String.valueOf(JS(expr[0], dataType).evaluate(emptyParams)));
  }

  public void putDefaultParam(String name, List<String> values)
  {
    putDefaultParam(name, values.toArray(new String[0]));
  }

  public boolean isDateFilterExpColType()
  {
    return this.getColumnType().equals(FilterType.FILTER_EXPRESSION.toString())
        && this.exprType == ExprType.GENERIC;
  }

  public boolean isNativeFilterExpColType()
  {
    return this.getColumnType().equals(FilterType.FILTER_EXPRESSION.toString())
        && (this.exprType == ExprType.NATIVE || this.exprType == ExprType.IMPORT);
  }


  public static boolean canIgnoreValues(String[] values)
  {
    return values == null || values.length == 0 || Strings.isNullOrEmpty(values[0]);
  }
  public boolean canIgnoreFilter()
  {
    return   !isNullOpFilter() && canIgnoreValues(values.toArray(new String[0])) && canIgnoreExpression(expr);
  }

  private boolean canIgnoreExpression(String expr) {
    return Strings.isNullOrEmpty(expr);
  }

  public boolean isNullOpFilter()
  {
    return EnumUtils.isValidEnum(FilterType.class, columnType) && !isDateRangeOrHistogram()
        && (operator.equals(Predicate.Type.is_null) || operator.equals(Predicate.Type.is_not_null));
  }
  public boolean isDateRangeOrHistogram()
  {
    return columnType.equals(GroupByType.DATE_HISTOGRAM.toString()) || columnType.equals(FilterType.FILTER_DATE_RANGE.toString());
  }

  public DataType type()
  {
    if(isDateFilterExpColType() || isNativeFilterExpColType())
    {
      if(MapUtils.isEmpty(operands))
        return DataType.STRING;
      else return DataType.INTEGER; //hardcorded here for now to make DATE_DIFF function work for vertica
      // correct fix: operands.values().stream().findFirst().get().type();
    }
    else return column.type();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public  static class MetaColumn {
    public final String name;
    public final String type;
    public final String dataType;
    public final String columnName;
    public final String format;
    public final String dimensionName;
    public final String hierarchyName;
    public final String dimensionColumnName;
    public final String factColumnName;
    public final Map<String,String> column;
    private Map<String,String> otherProps = Maps.newHashMap();
    private FactColumn factColumn;

    public MetaColumn(@JsonProperty("name") String name,@JsonProperty("type")  String type, @JsonProperty("dataType")String dataType,
        @JsonProperty("columnName") String columnName, @JsonProperty("format") String format, @JsonProperty("dimensionName") String dimensionName,
        @JsonProperty("hierarchyName") String hierarchyName, @JsonProperty("dimensionColumnName") String dimensionColumnName, @JsonProperty("factColumnName") String factColumnName,
        @JsonProperty("column") Map<String, String> column) {
      this.name =  name;
      this.type = type;
      this.dataType = dataType;
      this.columnName = columnName;
      this.format = format;
      this.dimensionName = dimensionName;
      this.hierarchyName = hierarchyName;
      this.dimensionColumnName = dimensionColumnName;
      this.factColumnName = factColumnName;
      this.column = column == null ? Maps.<String, String>newHashMap() : column;
    }
    public MetaColumn()
    {
      name = dataType = columnName = format = dimensionColumnName =
          dimensionName = hierarchyName = factColumnName = type =  null;
      column = null;
    }


    @JsonAnySetter
    public void set(String key, String value) {
      otherProps.put(key, value);
    }

    @JsonAnyGetter
    public Map<String,String> any() {
      return otherProps;
    }

    private DataType type()
    {
      String typeVal = ObjectUtils.firstNonNull(dataType, type, column.get("dataType"));

      if(typeVal == null && dimensionName != null)
      {
        typeVal = String.valueOf(dimensionName.equals("date_dim") ? DataType.DATE : DataType.DATETIME);
      }

      return Enum.valueOf(DataType.class, typeVal);
    }

    @JsonIgnore
    public FactColumn getFactColumn() {
      return factColumn;
    }

    @JsonIgnore
    public void setFactColumn(FactColumn factColumn) {
      this.factColumn = factColumn;
    }

    @JsonIgnore
    public String getColumnName() {
      return columnName;
    }
  }
}
