package com.flipkart.fdp.superbi.dsl.query.factory;

import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.exp.SelectColumnExp;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * User: shashwat
 * Date: 28/01/14
 */
@HasFactoryMethod
public class DSQueryBuilder {
    private final DSQuery.Builder builder = DSQuery.builder();
    private DSQueryBuilder() {}

    public static DSQueryBuilder select(SelectColumnExp... selectColumnExps) {
        final DSQueryBuilder queryBuilder = new DSQueryBuilder();
        List<SelectColumn> selectColumns = Lists.transform(Arrays.asList(selectColumnExps), new Function<SelectColumnExp, SelectColumn>() {
            @Nullable
            @Override
            public SelectColumn apply(@Nullable SelectColumnExp input) {
                return input.selectColumn;
            }
        });
        queryBuilder.builder.withColumns(selectColumns);
        return queryBuilder;
    }

    public static DSQueryBuilder select(List<SelectColumn> cols) {
        final DSQueryBuilder queryBuilder = new DSQueryBuilder();
        queryBuilder.builder.withColumns(cols);
        return queryBuilder;
    }

    public static DSQueryBuilder selectAll() {
        return new DSQueryBuilder();
    }

    public DSQueryBuilder where(Criteria criteria) {
        builder.withCriteria(criteria);
        return this;
    }

    public DSQueryBuilder within(DateRangePredicate... predicate) {
        builder.withDataRange(predicate);
        return this;
    }

    public DSQueryBuilder groupBy(String... groupByCols){
        builder.withGroupByColumns(Arrays.asList(groupByCols));
        return this;
    }

    public DSQueryBuilder groupBy(List<String> groupByCols){
        builder.withGroupByColumns(groupByCols);
        return this;
    }

    public DSQueryBuilder groupBy(ColumnExp... groupByCols){
        builder.withGroupByColumnExps(Arrays.asList(groupByCols));
        return this;
    }

    public DSQueryBuilder orderBy(String... orderByCols){
        builder.withOrderByColumns(Arrays.asList(orderByCols));
        return this;
    }

    public DSQueryBuilder orderBy(OrderByExp... orderByCols){
        builder.withOrderByColumnExps(Arrays.asList(orderByCols));
        return this;
    }

    public DSQueryBuilder orderBy(List<String> orderByCols){
        builder.withOrderByColumns(orderByCols);
        return this;
    }

    public DSQueryBuilder from(String fromTable) {
        builder.withFrom(fromTable);
        return this;
    }

    public DSQueryBuilder limit(Integer limit){
        builder.withLimit(limit);
        return this;
    }

    public DSQueryBuilder sample(Integer sample){
        builder.withSample(sample);
        return this;
    }

    public DSQuery build() {
        return builder.build();
    }

}
