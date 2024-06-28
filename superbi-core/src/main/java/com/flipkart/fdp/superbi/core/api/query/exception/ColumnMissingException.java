package com.flipkart.fdp.superbi.core.api.query.exception;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by akshaya.sharma on 10/07/19
 */

public class ColumnMissingException extends IllegalArgumentException {

  public static class MissingColumn {
    @JsonProperty
    private final String columnType;
    @JsonProperty
    private final String factName;
    @JsonProperty
    private final String factColumnName;
    @JsonProperty
    private final String dimensionName;
    @JsonProperty
    private final String dimensionHierarchyName;
    @JsonProperty
    private final String dimensionColumnName;

    @JsonCreator
    public MissingColumn (@JsonProperty("org") String columnType,
        @JsonProperty("factName") String factName,
        @JsonProperty("factColumnName") String factColumnName,
        @JsonProperty("dimensionName") String dimensionName,
        @JsonProperty("dimensionHierarchyName") String dimensionHierarchyName,
        @JsonProperty("dimensionColumnName") String dimensionColumnName) {
      this.columnType = columnType;
      this.factName = factName;
      this.factColumnName = factColumnName;
      this.dimensionName = dimensionName;
      this.dimensionHierarchyName = dimensionHierarchyName;
      this.dimensionColumnName = dimensionColumnName;
    }

    public String getColumnType() {
      return this.columnType;
    }

    public String getFactName() {
      return this.factName;
    }

    public String getFactColumnName() {
      return this.factColumnName;
    }

    public String getDimensionName() {
      return this.dimensionName;
    }

    public String getDimensionHierarchyName() {
      return this.dimensionHierarchyName;
    }

    public String getDimensionColumnName() {
      return this.dimensionColumnName;
    }

    public String toString()
    {
      return "Missing Column"
          + "\nFact Name: "+factName
          + "\nFact Column Name: " + factColumnName
          + "\nDimension Name: " + dimensionName
          + "\nDimension Hierarchy Name: " + dimensionHierarchyName
          + "\nDimension Column Name: " + dimensionColumnName;
    }
  }

  private MissingColumn missingColumn;

  public ColumnMissingException (String columnType, String factName, String factColumnName, String dimensionName, String dimensionHierarchyName, String dimensionColumnName)
  {
    this.missingColumn = new MissingColumn (columnType, factName, factColumnName, dimensionName, dimensionHierarchyName, dimensionColumnName);
  }

  public MissingColumn getMissingColumn() { return this.missingColumn; }
}