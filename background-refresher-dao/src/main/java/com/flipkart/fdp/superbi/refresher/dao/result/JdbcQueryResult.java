package com.flipkart.fdp.superbi.refresher.dao.result;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import lombok.SneakyThrows;

public abstract class JdbcQueryResult extends QueryResult{

  protected ResultSet resultSet;
  protected Connection connection;
  private enum IterationState { BEFORE_START, DURING, AFTER_END}
  private IterationState state = IterationState.BEFORE_START;
  protected List<String> schema = Lists.newArrayList();

  @Override
  public Iterator<List<Object>> iterator() {
    return new Iterator<List<Object>>() {
      @Override
      @SneakyThrows
      public boolean hasNext() {
        if(state == IterationState.BEFORE_START) {
          if(resultSet.next() == false) {
            state = IterationState.AFTER_END;
            return false;
          }
          state = IterationState.DURING;
          return true;
        }
        return state == IterationState.DURING;
      }

      @Override
      @SneakyThrows
      public List<Object> next() {
        final int columnCount = schema.size();
        final List<Object> row = Lists.newArrayList();
        for(int i = 1; i<=columnCount;i++)
          row.add(resultSet.getObject(i)) ;
        if(resultSet.next() == false) {
          state = IterationState.AFTER_END;
        }
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("This is a read only set, " +
            "Remove operation is not supported");
      }
    };
  }

  @Override
  public final List<String> getColumns() {
    return schema;
  }

}
