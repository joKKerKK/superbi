package com.flipkart.fdp.superbi.cosmos.data.api.execution.mysql;

import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import java.util.List;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Define;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;

/**
 * Created by amruth.s on 28/09/14.
 */

@UseStringTemplate3StatementLocator
public interface MysqlDao {

    @SqlQuery("<query>")
    @Mapper(ResultRow.Mapper.class)
    List<ResultRow> execute(@Define("query") String query);

    @SqlQuery("select count(*) from <schema>.<table>")
    Integer execute(@Define("schema") String schema, @Define("table") String table);

}
