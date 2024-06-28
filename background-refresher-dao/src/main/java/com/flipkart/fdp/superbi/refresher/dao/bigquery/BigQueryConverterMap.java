package com.flipkart.fdp.superbi.refresher.dao.bigquery;

import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.Maps;
import java.sql.Time;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.joda.time.DateTime;

/**
 * Created by mansi.jain on 25/03/22
 */
public class BigQueryConverterMap {

  private static final Map<LegacySQLTypeName, Function<FieldValue, Object>> typeToValueConverter = Maps.newHashMap();

  static {
    typeToValueConverter.put(LegacySQLTypeName.STRING,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null : fieldValue.getStringValue());
    typeToValueConverter.put(LegacySQLTypeName.BYTES,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null : fieldValue.getBytesValue());
    typeToValueConverter.put(LegacySQLTypeName.INTEGER,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null : fieldValue.getLongValue());
    typeToValueConverter.put(LegacySQLTypeName.FLOAT,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null : fieldValue.getDoubleValue());
    typeToValueConverter.put(LegacySQLTypeName.BOOLEAN,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null
            : fieldValue.getBooleanValue());
    typeToValueConverter.put(LegacySQLTypeName.NUMERIC,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null
            : fieldValue.getNumericValue());
    typeToValueConverter.put(LegacySQLTypeName.BIGNUMERIC,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null
            : fieldValue.getNumericValue());
    typeToValueConverter.put(LegacySQLTypeName.DATE,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null
            : fieldValue.getValue());
    typeToValueConverter.put(LegacySQLTypeName.TIMESTAMP,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null
            : Timestamp.ofTimeMicroseconds(fieldValue.getTimestampValue()).toDate());
    typeToValueConverter.put(LegacySQLTypeName.DATETIME,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null
            : new DateTime(fieldValue.getStringValue()));
    typeToValueConverter.put(LegacySQLTypeName.TIME,
        (fieldValue) -> Objects.isNull(fieldValue.getValue()) ? null
            : Time.valueOf(fieldValue.getStringValue()));
  }
  
  public static Map<LegacySQLTypeName, Function<FieldValue, Object>> getTypeToValueConverter() {
    return typeToValueConverter;
  }
}
