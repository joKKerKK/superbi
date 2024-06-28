package com.flipkart.fdp.superbi.core.api.query;

import static com.flipkart.fdp.superbi.core.util.FunctionUtils.forEach;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.core.api.query.column.ColumnFactory;
import com.flipkart.fdp.superbi.core.api.query.column.FactColumn;
import com.flipkart.fdp.superbi.core.api.query.exception.ColumnMissingException;
import com.flipkart.fdp.superbi.core.util.FunctionUtils;
import com.flipkart.fdp.superbi.cosmos.exception.HttpException;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections.MapUtils;

/**
 * Created by akshaya.sharma on 09/07/19
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class QueryPanel implements DefaultParamHolder{
  private List<PanelEntry> selectColumns;
  private List<PanelEntry> filterColumns;
  private List<PanelEntry> groupByColumns;
  private String fromTable;
  private View view;
  private int limit;
  private ColumnFactory.PanelContext context;
  private List<PanelEntry> missingColumns;
  private List<ColumnMissingException> missingColumnExceptions;
  private Optional<String> executionEngine = Optional.empty();

  @Override
  public void putDefaultParam(String name, String[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, String[]> getDefaultParams() {
    final Map<String,String[]> params = Maps.newHashMap();
    FunctionUtils.Block collect = new FunctionUtils.Block<DefaultParamHolder>() {
      @Override
      public void apply(DefaultParamHolder input) {
        params.putAll(input.getDefaultParams());
      }
    };
    forEach(selectColumns, collect);
    forEach(filterColumns, collect);
    forEach(groupByColumns, collect);

    params.put("limit", new String[] {String.valueOf(limit)});
    return params;
  }

  public void setAssociatedFactColumns()
  {
    context = new ColumnFactory.PanelContext(this);
    final List<ColumnMissingException> columnMissingExceptions = new ArrayList<>();
    final List<PanelEntry> missingColumns = new ArrayList<>();
    FunctionUtils.Block updateColDetails = new FunctionUtils.Block<PanelEntry>() {
      @Override
      public void apply(PanelEntry input) {
        try {
          input.setFactCol(FactColumn.valueOf(fromTable, input, context));
        }
        catch (ColumnMissingException ex) {
          columnMissingExceptions.add(ex);
          missingColumns.add(input);
        }
        input.setContainer(QueryPanel.this);
        if(!Objects.isNull(input.getCriteria()))
          input.getCriteria().setFactCol(FactColumn.valueOf(fromTable, input.getCriteria(), context));

        if(!MapUtils.isEmpty(input.getOperands()))
        {
          input.getOperands().forEach((operandName, metaColumn)->
              metaColumn.setFactColumn(FactColumn.valueOf(fromTable, metaColumn, context)));
        }
      }
    };

    forEach(selectColumns, updateColDetails);
    forEach(filterColumns, updateColDetails);
     forEach(groupByColumns, updateColDetails);
    this.missingColumns = missingColumns;
    this.missingColumnExceptions = columnMissingExceptions;
  }

  @JsonIgnore
  public void validateQueryPanelAndThrowException() {
    if (!getMissingColumnExceptions().isEmpty()) {
      List<ColumnMissingException.MissingColumn> missingColumns =
          getMissingColumnExceptions().stream().map(exception->exception.getMissingColumn()).collect(
              Collectors.toList());
      throw new HttpException(HttpException.SC_FIELD_NOT_FOUND, JsonUtil.toJson(missingColumns));
    }
  }

  @Override
  public Map<String, String[]> evalAndGetDefaultParams() {
    final Map<String,String[]> params = Maps.newHashMap();
    FunctionUtils.Block evalAndCollect = new FunctionUtils.Block<DefaultParamHolder>() {

      @Override
      public void apply(DefaultParamHolder input) {
        params.putAll(input.evalAndGetDefaultParams());
      }
    };
    forEach(selectColumns, evalAndCollect);
    forEach(filterColumns, evalAndCollect);
    forEach(groupByColumns, evalAndCollect);

    params.put("limit", new String[] {String.valueOf(limit)});
    return params;
  }

  @Override
  public void clearDefaultParams() {
    FunctionUtils.Block clear = new FunctionUtils.Block<DefaultParamHolder>() {
      @Override
      public void apply(DefaultParamHolder input) {
        input.clearDefaultParams();
      }
    };
    forEach(selectColumns, clear);
    forEach(filterColumns, clear);
    forEach(groupByColumns, clear);
  }

  @Data
  private static class View
  {
    private final boolean transpose;

    private View(@JsonProperty("transpose") boolean transpose) {
      this.transpose = transpose;
    }
  }
}
interface DefaultParamHolder
{
  void putDefaultParam(String name, String[] values);
  Map<String,String[]> getDefaultParams();
  Map<String,String[]> evalAndGetDefaultParams();
  void clearDefaultParams();
}
