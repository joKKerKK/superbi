package com.flipkart.fdp.superbi.cosmos.transformer;

import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CumulationResultIterator implements Iterator<List<Object>> {
  private final Iterator<List<Object>> backingIterator;
  private long processedCount;
  private List<Object> previousTransformedRow;
  private final List<SelectColumn> columnList;

  public CumulationResultIterator(Iterator<List<Object>> backingIterator,List<SelectColumn> columnList) {
    this.backingIterator = Preconditions.checkNotNull(backingIterator);
    this.columnList = Preconditions.checkNotNull(columnList);
  }

  @Override
  public boolean hasNext() {
    return this.backingIterator.hasNext();
  }

  @Override
  public List<Object> next() {
    List<Object> transformedRow =  transformRow(this.backingIterator.next());
    this.previousTransformedRow = transformedRow;
    this.processedCount++;
    return transformedRow;
  }


  enum Type {
    SUM_OR_COUNT {
      @Override
      public Double cumulate(Double previousValue, Double currentValue,
          long currentDataSize) {
        if(previousValue == null){
          previousValue = 0D;
        }
        if(currentValue == null) {
          currentValue = 0D;
        }

        return previousValue + currentValue;
      }
    },
    AVG {
      @Override
      public Double cumulate(Double previousValue, Double currentValue,
          long currentDataSize) {
        if(previousValue == null){
          previousValue = 0D;
        }
        if(currentValue == null) {
          currentValue = 0D;
        }
        return (previousValue*currentDataSize + currentValue)/(currentDataSize+1);
      }
    },
    MIN {
      @Override
      public Double cumulate(Double previousValue, Double currentValue,
          long currentDataSize) {
        if(previousValue == null){
          previousValue = Double.MAX_VALUE;
        }
        if(currentValue == null) {
          currentValue = Double.MAX_VALUE;
        }
        return previousValue > currentValue ? currentValue : previousValue;
      }
    },
    MAX {
      @Override
      public Double cumulate(Double previousValue, Double currentValue,
          long currentDataSize) {
        if(previousValue == null){
          previousValue = Double.MIN_VALUE;
        }
        if(currentValue == null) {
          currentValue = Double.MIN_VALUE;
        }
        return previousValue > currentValue ? previousValue : currentValue;
      }
    };

    public abstract Double cumulate(Double previousValue,Double currentValue,
        long currentDataSize);
    public static Type getFor(AggregationType type) {
      switch (type) {
        case AVG:
          return AVG;
        case SUM:
        case COUNT:
          return SUM_OR_COUNT;
        case MIN:
          return MIN;
        case MAX:
          return MAX;
        default:
          return SUM_OR_COUNT;
      }
    }
  }

  private List<Object> transformRow(List<Object> row) {
    List<Object> transformedRowList = new ArrayList<>();
    final AtomicInteger columnCount = new AtomicInteger(0);
    row.stream().forEach(value-> {
      final SelectColumn selectColumn = this.columnList.get(columnCount.get());
      if (selectColumn instanceof SelectColumn.Aggregation) {
        Double previousData = getPreviousColumnValue(columnCount.get());
        transformedRowList.add(
            Type.getFor(((SelectColumn.Aggregation) selectColumn).aggregationType)
                .cumulate(previousData,
                    getDoubleValue(value),this.processedCount));
      }else {
        transformedRowList.add(value);
      }
      columnCount.incrementAndGet();
    });
    return transformedRowList;
  }

  private Double getPreviousColumnValue(Integer columnCount) {
    return this.previousTransformedRow == null ? null :
        getDoubleValue(this.previousTransformedRow.get(columnCount));
  }

  private Double getDoubleValue(Object object){
    return object != null ? Double.valueOf(String.valueOf(object)) : null;
  }
}
