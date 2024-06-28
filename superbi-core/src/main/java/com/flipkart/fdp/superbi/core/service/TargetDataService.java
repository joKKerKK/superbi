package com.flipkart.fdp.superbi.core.service;

import static com.flipkart.fdp.superbi.dsl.query.factory.DSQueryBuilder.select;

import com.flipkart.fdp.auth.common.exception.NotAuthorizedException;
import com.flipkart.fdp.superbi.core.api.FactTargetMappingSTO;
import com.flipkart.fdp.superbi.core.api.ReportSTO;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import com.flipkart.fdp.superbi.core.context.ContextProvider;
import com.flipkart.fdp.superbi.core.model.DSQueryRefreshRequest;
import com.flipkart.fdp.superbi.core.model.QueryInfo;
import com.flipkart.fdp.superbi.core.model.QueryRefreshRequest;
import com.flipkart.fdp.superbi.core.model.ReportDataResponse;
import com.flipkart.fdp.superbi.core.model.TargetDataResponse;
import com.flipkart.fdp.superbi.core.model.TargetDataSeries;
import com.flipkart.fdp.superbi.core.model.TargetMapping;
import com.flipkart.fdp.superbi.core.util.DSQueryCriteria;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.flipkart.fdp.superbi.dsl.query.logical.AndLogicalOp;
import com.flipkart.fdp.superbi.dsl.query.predicate.InPredicate;
import com.flipkart.fdp.superbi.entities.ReportFederation;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 22/04/20
 */
@Slf4j
public class TargetDataService {
  private final DataService dataService;
  private final ReportService reportService;
  private final FactTargetMappingService factTargetMappingService;
  private final GringottsClient gringottsClient;

  private static final Gson gson = new Gson();

  private static final String TARGET_NAME_COLUMN_NAME = "event_name";
  private static final Long DEFAULT_TARGET_INTERVAL = 3600000l;

  @Inject
  public TargetDataService(DataService dataService,
      ReportService reportService,
      FactTargetMappingService factTargetMappingService,
      GringottsClient gringottsClient) {
    this.dataService = dataService;
    this.reportService = reportService;
    this.factTargetMappingService = factTargetMappingService;
    this.gringottsClient = gringottsClient;
  }

  public TargetDataResponse getTargetData(String org, String namespace, String name,
      Map<String, String[]> params) {
    ReportSTO reportSTO = reportService.find(org, namespace, name);
    TargetDataResponse targetDataResponse = new TargetDataResponse();

    ClientPrivilege clientPrivilege =
        ContextProvider.getCurrentSuperBiContext().getClientPrivilege();

    final String userName = ContextProvider.getCurrentSuperBiContext().getUserName();

    final boolean isTargetUser = gringottsClient.isUserPartOfRole(
        userName, "targetUser");

    if (!isTargetUser) {
      throw new NotAuthorizedException("You are not allowed to view targets");
    }

    String reportFactName = reportSTO.getQueryFormMetaDetailsMap().getFromTable();

    Map<String, Object> additionalAttributes = reportSTO.getAdditionalReportAttributes();
    List<TargetMapping> targetMappings = gson.fromJson(
        additionalAttributes.getOrDefault("targetMappings", Collections.EMPTY_LIST).toString(),
        new TypeToken<ArrayList<TargetMapping>>() {
        }.getType());

    // Find downasampling info
    String downsamplingColumnName = StringUtils.EMPTY;
    Long downsamplingInterval = 3600000L;

    for (Map.Entry<String, String[]> entry : params.entrySet()) {
      String paramName = entry.getKey().toString();

      if (!paramName.endsWith(".downsample")) {
        continue;
      }

      paramName = paramName.split("\\.downsample")[0];

      if (paramName.indexOf('.') == -1) {
        paramName = reportFactName + "." + paramName;
      }

      try {
        downsamplingColumnName = paramName;
        downsamplingInterval = Long.parseLong(entry.getValue()[0]);
      } catch (Exception e) {
        downsamplingColumnName = StringUtils.EMPTY;
        downsamplingInterval = 3600000L;
      }
      break;
    }

    // Prepare target fact filters from report's fact filter
    for (TargetMapping mapping : targetMappings) {
      // Prepare dictionary of mapped target columns from report's underlying fact
      List<FactTargetMappingSTO> factTargetmappings =
          factTargetMappingService.getTargetMappingsForFactAndTargetFact(
              reportFactName, mapping.getTargetFact().getName());

      Map<String, String> factColumnTargetColumnDictionary =
          prepareFactColumnTargetColumnDictionary(
              factTargetmappings);

      List requestCriterias = createCriteriaForTargetFact(reportFactName,
          factColumnTargetColumnDictionary, params);


      TargetDataResponse targetDataForMapping = getTargetDataForMapping(mapping,
          requestCriterias, factColumnTargetColumnDictionary, downsamplingColumnName,
          downsamplingInterval, clientPrivilege, reportSTO.getName());
      targetDataResponse.merge(targetDataForMapping);
    }

    SelectColumn.SeriesType seriesType = params.containsKey(
        "seriesType") ? SelectColumn.SeriesType.valueOf(
        params.get("seriesType")[0]) : SelectColumn.SeriesType.INSTANTANEOUS;
    computeTargetData(targetDataResponse, seriesType);

    Date now = new Date();
    getTargetAsOfNow(seriesType, targetDataResponse, now);

    List<ReportDataResponse> queryResponses = targetDataResponse.getQueryResponses();

    queryResponses = queryResponses.stream()
        .filter(queryResponse -> {
          boolean isDataAvailable = queryResponse.getQueryCachedResult() != null;
          boolean isReportLocked = QueryInfo.DATA_CALL_TYPE.QUERY_LOCKED.equals(queryResponse.getDataCallType());
          return isDataAvailable || isReportLocked;
        })
        .collect(Collectors.toList());

    targetDataResponse.setQueryResponses(queryResponses);

    if(targetMappings.isEmpty()) {
      targetDataResponse.setDataCallType(QueryInfo.DATA_CALL_TYPE.QUERY_LOCKED);
    }

    return targetDataResponse;
  }

  @SneakyThrows
  public TargetDataResponse getTargetDataForMapping(TargetMapping mapping,
      List<Criteria> rootCriterias, Map<String, String> factColumnTargetColumnDictionary,
      String downsamplingColumnName, Long downsamplingInterval, ClientPrivilege clientPrivilege, String reportName) {
    Preconditions.checkNotNull(mapping, "Cant find targets for invalid/null mapping");

    TargetMapping.TargetFact targetFact = mapping.getTargetFact();
    List<String> selectColumnNames = Lists.newArrayList(TARGET_NAME_COLUMN_NAME);

    String baseColumnName = targetFact.getBaseColumnName();
    selectColumnNames.add(baseColumnName);

    String downsamplingTargetColumnName = StringUtils.EMPTY;

    if (StringUtils.isNotBlank(
        downsamplingColumnName) && factColumnTargetColumnDictionary.containsKey(
        downsamplingColumnName)) {
      downsamplingTargetColumnName = factColumnTargetColumnDictionary.get(downsamplingColumnName);
    }

    boolean isDownSamplingRequired = StringUtils.isNotBlank(
        downsamplingTargetColumnName) && downsamplingTargetColumnName.endsWith(
        "." + baseColumnName);

    List<String> finalSelectColumnNames = Lists.newArrayList();

    List<SelectColumn> selectColumns = selectColumnNames.stream().distinct().map(
        colName -> {
          finalSelectColumnNames.add(colName);
          return ExprFactory.SEL_COL(colName).selectColumn;
        }).collect(Collectors.toList());


    List<String> targetColumns = targetFact.getColumns();
//    selectColumnNames.addAll(targetColumns);

    if (targetColumns.contains(baseColumnName)) {
      throw new UnsupportedOperationException("Base column can not be a part of select columns");
    }

    List<SelectColumn> aggregatedColumns = targetColumns.stream().distinct().map(colName -> {
      finalSelectColumnNames.add(colName);
      return ExprFactory.AGGR(colName, AggregationType.SUM).selectColumn;
    }).collect(Collectors.toList());

    selectColumns.addAll(aggregatedColumns);

    List criteria = Lists.newArrayList(
        new InPredicate(ExprFactory.COL(TARGET_NAME_COLUMN_NAME),
            mapping.getTargetNames().stream().map(
                targetName -> ExprFactory.LIT(targetName)).collect(Collectors.toList()))
    );
    if (rootCriterias != null) {
      criteria.addAll(rootCriterias);
    }

    int baseColumnIndex = 1;

    Optional<ReportFederation> reportFederation = dataService
        .getFederationFromTable(targetFact.getName(),
            clientPrivilege.getReportAction());
    String storeIdentifier = dataService.getStoreIdentifierForFact(mapping.getTargetFact().getName(),
        reportFederation,Optional.empty());

    DSQuery dsQuery = select(
        selectColumns
    ).from(targetFact.getName())
        .where(new AndLogicalOp(
            criteria
        )).groupBy(selectColumnNames)
        .orderBy(baseColumnName)
        .build();

    QueryRefreshRequest queryRefreshRequest = DSQueryRefreshRequest.builder()
        .dsQuery(dsQuery)
        .dateRange(Maps.newHashMap())
        .params(Maps.newHashMap())
        .appliedFilters(Maps.newHashMap())
        .storeIdentifier(storeIdentifier)
        .federationProperties(reportFederation.map(ReportFederation::getFederationProperties).orElse(Maps.newHashMap()))
        .reportName(Optional.of(reportName))
        .build();

    ReportDataResponse fetchQueryResponse = dataService.executeQuery(queryRefreshRequest);


    Map<String, List<Object[]>> data = new HashMap<String, List<Object[]>>();
    List<TargetDataSeries> targetDataSeries = Lists.newArrayList();
    Map<String, Object> targetAsOfNow = new HashMap<String, Object>();

    Map<String, Map<Object, Long>> metricBucketRepository = new HashMap<>();


    List<String> selectTargetNames = mapping.getTargetNames();
    List<String> selectedMetrics = mapping.getTargetFact().getColumns();

    for (String selectedTargetName : selectTargetNames) {
      for (String selectedMetric : selectedMetrics) {
        if (selectedMetric.equals(baseColumnName)) {
          continue;
        }
        String label = selectedTargetName + " - " + selectedMetric;
        String key = getMD5Hash(selectedTargetName + selectedMetric + targetFact.getName());

        TargetDataSeries dataSeriesIdentifier = new TargetDataSeries(targetFact.getName(),
            selectedTargetName,
            selectedMetric, label.toUpperCase(), key);

        targetDataSeries.add(dataSeriesIdentifier);
        data.put(dataSeriesIdentifier.getKey(), Lists.newArrayList());
        metricBucketRepository.put(dataSeriesIdentifier.getKey(), new LinkedHashMap<>());
      }
    }

    if (fetchQueryResponse.getQueryCachedResult() != null) {
      for (List<Object> tuple :
          fetchQueryResponse.getQueryCachedResult().getQueryResult().getData()) {

        Object targetNameValue = tuple.get(0);

        for (int i = 1; i < tuple.size(); i++) {
          SelectColumn column = selectColumns.get(i);
          Object columnValue = tuple.get(i);

          String metricName = finalSelectColumnNames.get(i);

          if (metricName.equals(baseColumnName)) {
            continue;
          }

          try {
            columnValue = Long.parseLong(String.valueOf(columnValue));
          } catch (NumberFormatException numEx) {
            columnValue = 0L;
          }


          String label = targetNameValue + " - " + metricName;
          String key = getMD5Hash(targetNameValue + metricName + targetFact.getName());

          TargetDataSeries dataSeriesIdentifier = new TargetDataSeries(targetFact.getName(),
              targetNameValue.toString(),
              metricName, label.toUpperCase(), key);

          Map<Object, Long> metricBuckets = metricBucketRepository.get(
              dataSeriesIdentifier.getKey());

          if (isDownSamplingRequired) {
            Object metricBucketPoint = getBucketPoint(tuple.get(baseColumnIndex),
                downsamplingInterval);
            Long metricBucketPointValue = 0L;
            if (metricBuckets.containsKey(metricBucketPoint)) {
              metricBucketPointValue = metricBuckets.get(metricBucketPoint);
            }

            metricBucketPointValue += (Long) columnValue;
            metricBuckets.put(metricBucketPoint, metricBucketPointValue);
          } else {
            metricBuckets.put(tuple.get(baseColumnIndex), (Long) columnValue);
          }
        }
      }
    }

    for (Map.Entry<String, Map<Object, Long>> bucketEntry : metricBucketRepository.entrySet()) {
      List<Object[]> dataSeries = data.get(bucketEntry.getKey());
      for (Map.Entry<Object, Long> dataPoint : bucketEntry.getValue().entrySet()) {
        dataSeries.add(Lists.newArrayList(dataPoint.getKey(), dataPoint.getValue()).toArray());
      }
    }
    TargetDataResponse targetDataResponse = TargetDataResponse.builder()
        .data(data)
        .dataSeries(targetDataSeries)
        .queryResponses(Lists.newArrayList(fetchQueryResponse))
        .targetMappings(Lists.newArrayList(mapping))
        .targetAsOfNow(targetAsOfNow)
        .build();

    return targetDataResponse;
  }

  public static List createCriteriaForTargetFact(String factName,
      Map<String, String> factColumnTargetColumnDictionary, Map<String, String[]> params) {
    List criteriaList = Lists.newArrayList();
    for (Map.Entry<String, String[]> entry : params.entrySet()) {
      String paramName = entry.getKey().toString();
      String[] parts = paramName.split("\\$operator\\$");

      String columnParamName = parts[0];
      columnParamName = columnParamName.split("\\.endTimestamp")[0];
      columnParamName = columnParamName.split("\\.startTimestamp")[0];

      if (columnParamName.indexOf('.') == -1) {
        columnParamName = factName + "." + columnParamName;
      }

      String originalColumnParamName = columnParamName;

      if (factColumnTargetColumnDictionary.containsKey(columnParamName)) {
        columnParamName = factColumnTargetColumnDictionary.get(columnParamName);
      } else {
        continue;
      }

      if (paramName.endsWith("$operator$")) {
        // Query param for criteria operator
//        columnName = columnName.contains(".") ? columnName : factName + "." + columnName;
        String[] operatorValues = entry.getValue();
        if (operatorValues != null && operatorValues.length > 0) {
          String operator = operatorValues[0];
          DSQueryCriteria reportCriteria = DSQueryCriteria.valueOf(operator);
          String[] rightOperands = params.get(originalColumnParamName);
          String[] columnNameParts = columnParamName.split("\\.");
          String columnName = columnNameParts.length > 0 ? columnNameParts[1] : columnParamName;

          Criteria criteria = reportCriteria.getCriteria(columnName, rightOperands);
          if (criteria != null) {
            criteriaList.add(criteria);
          }
        }
      } else if (paramName.endsWith(".startTimestamp")) {
        String[] timstampValues = entry.getValue();
        DSQueryCriteria reportCriteria = DSQueryCriteria.dgte;
        String[] columnNameParts = columnParamName.split("\\.");
        String columnName = columnNameParts.length > 0 ? columnNameParts[1] : columnParamName;

        Criteria criteria = reportCriteria.getCriteria(columnName, timstampValues);
        if (criteria != null) {
          criteriaList.add(criteria);
        }
      } else if (paramName.endsWith(".endTimestamp")) {
        String[] timstampValues = entry.getValue();
        DSQueryCriteria reportCriteria = DSQueryCriteria.dlt;
        String[] columnNameParts = columnParamName.split("\\.");
        String columnName = columnNameParts.length > 0 ? columnNameParts[1] : columnParamName;

        Criteria criteria = reportCriteria.getCriteria(columnName, timstampValues);
        if (criteria != null) {
          criteriaList.add(criteria);
        }
      }
    }
    return criteriaList;
  }

  private static String getMD5Hash(String data) {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      byte[] hash = digest.digest(data.getBytes("UTF-8"));
      return DatatypeConverter.printHexBinary(hash); // make it printable
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Map<String, String> prepareFactColumnTargetColumnDictionary(
      List<FactTargetMappingSTO> factTargetmappings) {
    Preconditions.checkNotNull(factTargetmappings, "factTargetmappings can not be null in {}",
        factTargetmappings);

    Map<String, String> dictionary = new HashMap<String, String>();
    for (FactTargetMappingSTO factTargetmapping : factTargetmappings) {
      String key = factTargetmapping.getFactName() + "." + factTargetmapping.getFactColumnName();
      String value =
          factTargetmapping.getTargetFactName() + "." + factTargetmapping.getTargetColumnName();
      dictionary.put(key, value);
    }
    return dictionary;
  }

  public static Object getBucketPoint(Object objectValue, Long interval) {
    Preconditions.checkNotNull(objectValue);
    Preconditions.checkNotNull(interval);

    if (objectValue instanceof Number) {
      Preconditions.checkArgument(interval < ((Long) objectValue).longValue());
      return interval * (((Long) objectValue) / interval);
    }

    // A date case

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    sdf.setTimeZone(TimeZone.getTimeZone("IST"));

    try {
      Date date = sdf.parse(objectValue.toString());
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      int offset = calendar.getTimeZone().getRawOffset();
      Long effectiveTime = calendar.getTimeInMillis() + offset;
      Long bucketTime = interval * (effectiveTime / interval) - offset;
      return sdf.format(new Date(bucketTime));
    } catch (ParseException e1) {
      log.info(e1.getMessage(), e1);
    }
    return objectValue;
  }

  //TODO: Revisit for multiDate targets
  public static void computeTargetData(TargetDataResponse targetDataResponse,
      SelectColumn.SeriesType seriesType) {
    /*
      Only process if value of a target if of number type, else just ignore and series is cumulative
     */
    if (seriesType != SelectColumn.SeriesType.CUMULATIVE) {
      return;
    }

    for (List<Object[]> targetPoints : targetDataResponse.getData().values()) {
      Long cumulativeValue = 0L;
      try {
        for (Object[] targetPoint : targetPoints) {
          if (targetPoint.length < 2) {
            throw new Exception("Incorrect target point");
          }
          Object targetValue = targetPoint[1];
          if (targetValue instanceof Number) {
            cumulativeValue += ((Number) targetValue).longValue();
            targetPoint[1] = cumulativeValue;
          } else {
            throw new NumberFormatException("Not a valid number " + String.valueOf(targetValue));
          }
        }
      } catch (Exception ex) {
        // Values were not numbers or did not follow pattern pf [x, y], ignore current series and
        // move to next data series
        log.debug("Can not cumulate the data series", ex);
      }
    }
  }

  public static void getTargetAsOfNow(SelectColumn.SeriesType seriesType,
      TargetDataResponse targetDataResponse, Date now) {
    /*
      Only process if value of a target is of number type and value of x-axis is of Date type
     */

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    sdf.setTimeZone(TimeZone.getTimeZone("IST"));

    for (Map.Entry<String, List<Object[]>> targetSeries : targetDataResponse.getData().entrySet()) {
      String targetSeriesKey = targetSeries.getKey();
      List<Object[]> targetPoints = targetSeries.getValue();
      Long targetAsOfNow = 0L;

      try {

        Long targetInterval = targetPoints.size() >= 2 ?
            sdf.parse(String.valueOf(targetPoints.get(1)[0])).getTime() -
                sdf.parse(String.valueOf(targetPoints.get(0)[0])).getTime() :
            DEFAULT_TARGET_INTERVAL;


        for (int i = 0; i < targetPoints.size(); i++) {
          Object[] targetPoint = targetPoints.get(i);
          if (targetPoint.length < 2) {
            throw new Exception("Incorrect target point");
          }
          Object targetAt = targetPoint[0];
          Date targetAtDate = sdf.parse(String.valueOf(targetAt));
          Object targetValue = targetPoint[1];
          if (!(targetValue instanceof Number)) {
            throw new NumberFormatException("Not a valid number " + String.valueOf(targetValue));
          }

          // if currentDate is after the targetedDate
          if (now.compareTo(targetAtDate) > 0) {

            Date nextTargetAtDate = new Date(targetAtDate.getTime() + targetInterval);
            if (i < targetPoints.size() - 1) {
              Object[] nextTargetPoint = targetPoints.get(i + 1);
              Object nextTargetAt = nextTargetPoint[0];
              nextTargetAtDate = sdf.parse(String.valueOf(nextTargetAt));
            }

            if (now.compareTo(nextTargetAtDate) < 0) {
              Float currDiff = (float) (now.getTime() - targetAtDate.getTime());
              Float actualDiff = (float) (nextTargetAtDate.getTime() - targetAtDate.getTime());

              Long linearEstimatedValue =
                  (long) ((currDiff / actualDiff) * ((Number) targetValue).floatValue());

              if (seriesType != SelectColumn.SeriesType.CUMULATIVE) {
                targetAsOfNow += linearEstimatedValue;
              } else {
                targetAsOfNow = linearEstimatedValue;
              }
            } else {
              if (seriesType != SelectColumn.SeriesType.CUMULATIVE) {
                targetAsOfNow += ((Number) targetValue).longValue();
              } else {
                targetAsOfNow = ((Number) targetValue).longValue();
              }
            }
          } else {
            break;
          }
        }

        targetDataResponse.getTargetAsOfNow().put(targetSeriesKey, targetAsOfNow);
      } catch (Exception ex) {
        // if x-axis was not a date type, ignore and move to next series
        // Values were not numbers or did not follow pattern pf [x, y], ignore current series and
        // move to next data series
        log.debug("Can not cumulate the data series", ex);
      }
    }
  }
}
