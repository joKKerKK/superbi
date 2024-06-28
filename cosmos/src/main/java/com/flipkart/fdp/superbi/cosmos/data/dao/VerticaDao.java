package com.flipkart.fdp.superbi.cosmos.data.dao;

import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import java.util.List;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Define;
import org.skife.jdbi.v2.sqlobject.customizers.FetchSize;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;

/**
 * User: aniruddha.gangopadhyay
 * Date: 06/03/14
 * Time: 1:30 PM
 */
@UseStringTemplate3StatementLocator
public interface VerticaDao {

    int chunkSizeInRows = 10000;  // figure out a way to inject this through annotations

    @SqlQuery("<query>")
    @Mapper(ResultRow.Mapper.class)
    List<ResultRow> execute(@Define("query") String query);

    @SqlQuery("<query>")
    @Mapper(ResultRow.StreamMapper.class)
    @FetchSize(chunkSizeInRows)
    Iterable<List<Object>> getStreamingResultSet(@Define("query") String query);

    @SqlQuery("select count(*) from <schema>.<table>")
    Integer execute(@Define("schema") String schema, @Define("table") String table);

}
