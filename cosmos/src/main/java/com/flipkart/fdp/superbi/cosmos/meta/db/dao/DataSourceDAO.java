package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static com.flipkart.fdp.superbi.cosmos.meta.model.data.DataSource.Type.raw;
import static org.hibernate.criterion.Restrictions.*;
import static org.hibernate.criterion.Restrictions.and;
import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.DataSource;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.DataSource.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Query;
import org.hibernate.SessionFactory;

/**
 * Created by amruth.s on 08/12/14.
 * This is a generic datasource dao that can be used with any entity that extends datasource
 */
public class DataSourceDAO extends AbstractDAO<DataSource> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public DataSourceDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public DataSource getDataSourceByName(String name) {
        return uniqueResult(criteria().add(and(eq("name", name),eq("deleted", false))));
    }

    public DataSource getBy(Type type, String name) {
        return uniqueResult(
                criteria().add(
                        and(
                                eq("deleted", false),
                                eq("type", type),
//                                eq("namespace.org.name", org),
//                                eq("namespace.name", namespace),
                                eq("name", name)
                        )

                )
        );
    }

    private List<String> getPredicates(Set<String> resourceURIList) {
        final List<String> predicates = Lists.newArrayList();
        for(String resource : resourceURIList) {
            String escapedResource = resource.replace("'", "''");
            if(resource.contains("*")) {

                predicates.add(
                        "concat(ds.namespace.org.name ,'/', ds.namespace.name , '/', ds.name) like '" + escapedResource.replace("*", "%")+ "'"
                );
            } else {
                predicates.add(
                        "concat(ds.namespace.org.name , '/', ds.namespace.name , '/', ds.name) like '" + escapedResource + "'"
                );
            }
        }
        return predicates;
    }

    public List<DataSource> getMatching(Set<String> resourceURIList, Map<String, String[]> filters, int offset, int limit) {

        final StringBuilder hql = new StringBuilder("FROM com.flipkart.fdp.superbi.cosmos.meta.model.data.DataSource ds ");

        if (filters.containsKey("store")) {
            hql.append("WHERE ds.class = Fact AND (");
            String[] stores = filters.get("store");
            for (int i = 0 ; i < stores.length ; i++) {
                hql.append("ds.table.source.sourceType LIKE '%").append(stores[i]).append("%' ");
                if (i < stores.length - 1)
                    hql.append("OR ");
            }
            hql.append(") AND ");
        }
        else
            hql.append("WHERE ");

        hql.append("ds.deleted=false ");

        if(filters.containsKey("type")) {
            hql.append("and ds.type = ").append(Type.valueOf(filters.get("type")[0]).ordinal()).append(" ");
            if (Type.valueOf(filters.get("type")[0]) == raw) {
                hql.append("and ds.published=true ");
            }
        }
        else {
            //For no type filter case, showing only tables that are created by user aka Published Table
            hql.append("and (ds.class != Table or ds.published=true) ");
        }

        if(filters.containsKey("filter")) {
            final List<String> searchPredicates = getPredicates(Sets.newHashSet(filters.get("filter")[0].split(",")));
            hql.append("and (");
            Joiner.on(" or ").appendTo(hql, searchPredicates);
            hql.append(")");
        }

        if(filters.containsKey("preferences")) {
            final List<String> searchPredicates = getPredicates(Sets.newHashSet(filters.get("preferences")));
            hql.append("and (");
            Joiner.on(" or ").appendTo(hql, searchPredicates);
            hql.append(")");
        }

        if(!resourceURIList.isEmpty()) {
            final List<String> baseFilterPredicates = getPredicates(resourceURIList);
            hql.append("and (");
            Joiner.on(" or ").appendTo(hql, baseFilterPredicates);
            hql.append(")");
        }

        final Query query = currentSession().createQuery(hql.toString());
        query.setFirstResult(offset);
        if(limit!=-1) query.setMaxResults(limit);

        return query.list();
    }

}
