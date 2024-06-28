package com.flipkart.fdp.superbi.cosmos.data.dao;

/**
 * User: aniruddha.gangopadhyay
 * Date: 06/03/14
 * Time: 12:06 PM
 */

import com.flipkart.fdp.superbi.cosmos.meta.db.DatabaseConfiguration;
import javax.sql.DataSource;
import org.skife.jdbi.v2.DBI;

public class DBIFactory {

    private final DataSourceFactory dataSourceFactory = new DataSourceFactory();

    public DBI build(DatabaseConfiguration configuration) throws ClassNotFoundException {
        final DataSource dataSource = dataSourceFactory.build(configuration);
        return build(dataSource);
    }

    public DBI build(DataSource dataSource) {
        return new DBI(dataSource);
    }
}
