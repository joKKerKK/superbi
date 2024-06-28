package com.flipkart.fdp.superbi.core.util;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.fdp.superbi.core.api.query.PanelEntry;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.ServerSideTransformer;
import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Table;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Table.Column;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn.DateHistogram;
import com.flipkart.fdp.superbi.entities.ReportFederation;
import com.flipkart.fdp.superbi.refresher.api.result.query.RawQueryResult;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Created by akshaya.sharma on 17/07/19
 */
@Slf4j
public class QueryUtil {
  public static com.google.common.base.Optional<Integer> extractLimit(
      Map<String, String[]> params) {
    com.google.common.base.Optional<Integer> limit = com.google.common.base.Optional.absent();
    if (params.containsKey("limit") && params.get("limit").length > 0
        && !Strings.isNullOrEmpty(params.get("limit")[0])) {
      limit = com.google.common.base.Optional.of(Integer.valueOf(params.get("limit")[0]));
    }
    return limit;
  }

  private static Map<String, String[]> prepareParamsBeforeCall(Map<String, String[]> params) {
    Map<String, String[]> nonEmptyParams = Maps.newHashMap();
    //remove the empty params
    for (Iterator<Map.Entry<String, String[]>> it = params.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, String[]> entry = it.next();
      if (!PanelEntry.canIgnoreValues(entry.getValue())) {
        nonEmptyParams.put(entry.getKey(), entry.getValue());
      }
    }

    //Temp Hack to solve cosmos issue where date range is not working when you pass empty params
    // => com.flipkart.bigfoot.cosmos.data.query.query.DSQuery.accept()
    nonEmptyParams.put("DummyParam" + System.currentTimeMillis(), new String[0]);
    return nonEmptyParams;
  }

  @NotNull
  public static Map<String, String[]> preparePrams(Map<String, String[]> params,
      DSQueryBuilder.QueryAndParam queryAndParamOp) {
    /**
     * Code copied from resourceSchedule:88
     */
    Map<String, String[]> runParameters = queryAndParamOp.getParams();
    runParameters.putAll(params);

    return prepareParamsBeforeCall(runParameters);
  }


  public static Schema buildSchema(DSQuery query, Map<String, String[]> params) {
    final Schema schema = query.getSchema(params, true);
    List<DataType> dataTypes = getDataTypesForColsIn(query);
    int i=0;
    for(SelectColumn col: schema.columns) {
      col.setDataType(dataTypes.get(i++));
    }
    return schema;
  }

  private static List<DataType> getDataTypesForColsIn(DSQuery query) {
    final MetaAccessor metaAccessor = MetaAccessor.get();
    final List<DataType> dataTypes = Lists.newArrayList();
    final List<Table.Column> columns = metaAccessor.getEntityColumns(query.getFromTable());
    final List<String> columnNames = Lists.transform(columns, getName);
    for (SelectColumn queryCol : query.getVisibleSelectedColumns()) {
      switch(queryCol.type) {
        case AGGREGATION:
        {
          SelectColumn.Aggregation aggregation = ((SelectColumn.Aggregation) queryCol);
          AggregationType aggrType = aggregation.aggregationType;
          int index = columnNames.indexOf(getColumnName(aggregation.colName));
          if(index == -1 || aggrType == AggregationType.COUNT || aggrType == AggregationType.DISTINCT_COUNT )
            //When count or distinct_count is used we are passing the result as it is
            dataTypes.add(DataType.LONG);
          else
            dataTypes.add(columns.get(index).getType());

        }
        break;
        case SIMPLE:
        {int index = columnNames.indexOf(getColumnName(((SelectColumn.SimpleColumn)queryCol).colName));
          dataTypes.add(index == -1 ? DataType.STRING : columns.get(index).getType());}
        break;
        case DATE_HISTOGRAM:
        {
          int index = columnNames.indexOf(getColumnName(((DateHistogram)queryCol).columnName));
          dataTypes.add(index == -1 ? DataType.STRING : columns.get(index).getType());
        }
        break;
        case DERIVED:
        default:
          dataTypes.add(DataType.STRING);
      }
    }
    return dataTypes;
  }

  static Function<Column, String> getName = new Function<Table.Column, String>() {
    @Nullable
    @Override
    public String apply(Table.Column input) {
      return input.getName();
    }
  };

  private static String getColumnName(String qualifiedColName) {
    String[] parts = qualifiedColName.split("\\.");
    return parts[parts.length-1];
  }

}
