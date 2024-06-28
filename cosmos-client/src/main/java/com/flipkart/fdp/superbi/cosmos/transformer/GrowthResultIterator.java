package com.flipkart.fdp.superbi.cosmos.transformer;

import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class GrowthResultIterator implements Iterator<List<Object>> {

  private List<Object> previousRow;
  private final Iterator<List<Object>> backingIterator;
  private final List<SelectColumn> columnList;

  public GrowthResultIterator(Iterator<List<Object>> backingIterator,List<SelectColumn> columnList) {
    this.backingIterator = Preconditions.checkNotNull(backingIterator);
    this.columnList = Preconditions.checkNotNull(columnList);
  }

  @Override
  public boolean hasNext() {
    return this.backingIterator.hasNext();
  }

  @Override
  public List<Object> next() {
    List<Object> currentRow = backingIterator.next();
    List<Object> transformedRow = transformRow(currentRow);
    this.previousRow = currentRow;
    return transformedRow;
  }

  private Double calculate(Double previousValue, Double currentValue){
    if(previousValue == null || currentValue == null){
      return 0D;
    }
    return ((currentValue - previousValue)/previousValue)*100;
  }

  private List<Object> transformRow(List<Object> row) {
    List<Object> transformedRowList = new ArrayList<>();
    final AtomicInteger columnCount = new AtomicInteger(0);
    row.stream().forEach(value-> {
      final SelectColumn selectColumn = this.columnList.get(columnCount.get());
      if (selectColumn instanceof SelectColumn.Aggregation) {
        Double previousData = getPreviousColumnValue(columnCount.get());
        transformedRowList.add(calculate(previousData,getDoubleValue(row.get(columnCount.get()))));
      }else {
        transformedRowList.add(value);
      }
      columnCount.incrementAndGet();
    });
    return transformedRowList;
  }

  private Double getPreviousColumnValue(Integer columnCount) {
    return this.previousRow == null ? null :
        getDoubleValue(this.previousRow.get(columnCount));
  }

  private Double getDoubleValue(Object object){
    return object != null ? Double.valueOf(String.valueOf(object)) : null;
  }

}
