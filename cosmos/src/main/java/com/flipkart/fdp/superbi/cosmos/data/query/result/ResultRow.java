package com.flipkart.fdp.superbi.cosmos.data.query.result;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

/**
 * User: aniruddha.gangopadhyay
 * Date: 10/02/14
 * Time: 12:30 PM
 */
public class ResultRow  implements Serializable {
    public List<Object> row = Lists.newArrayList();

    public ResultRow(List<Object> row) {
        this.row = row;
    }

    public ResultRow() {
    }

    public static class Mapper implements ResultSetMapper<ResultRow>{

        @Override
        public ResultRow map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            ResultSetMetaData metaData = r.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<Object> row = Lists.newArrayList();
            for(int i = 1; i<=columnCount;i++)
                row.add(r.getObject(i)) ;
            return new ResultRow(row);
        }
    }

    public static class StreamMapper implements ResultSetMapper<List<Object>>{

        @Override
        public List<Object> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            ResultSetMetaData metaData = r.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<Object> row = Lists.newArrayList();
            for(int i = 1; i<=columnCount;i++)
                row.add(r.getObject(i)) ;
            return row;
        }
    }

    @Override
    public String toString() {
        return Joiner.on(',').skipNulls().join(row); // coma separated row values
    }
}
