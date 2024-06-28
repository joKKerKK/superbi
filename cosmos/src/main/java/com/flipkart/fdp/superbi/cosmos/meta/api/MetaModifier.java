package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.db.TransactionLender;
import com.flipkart.fdp.superbi.cosmos.meta.db.WorkUnit;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Source;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Table;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.hibernate.*;
import org.slf4j.LoggerFactory;

/**
 * User: aniruddha.gangopadhyay
 * Date: 04/03/14
 * Time: 4:15 PM
 */
public class MetaModifier {
    private SessionFactory sessionFactory;
    private TransactionLender transactionLender;

    private static MetaModifier defaultInstance;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MetaModifier.class);

    public static void initialize(SessionFactory sessionFactory) {
        defaultInstance = new MetaModifier(sessionFactory);
    }

    public static MetaModifier get() {
        if(defaultInstance == null) {
            throw new RuntimeException("MetaModifier is not initialized!");
        }
        return defaultInstance;
    }

    public MetaModifier(SessionFactory sessionFactory){
        this.sessionFactory = sessionFactory;
        this.transactionLender = new TransactionLender(sessionFactory);
    }

    public void deleteNamespace(final String namespace, final String org){
        final NamespaceDAO dao = new NamespaceDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.delete(namespace,org);
            }
        });
    }

    public void deleteOrg(final String org){
        final OrgDAO dao = new OrgDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.delete(org);
            }
        });
    }

    public void deleteSource(final String sourceName){
        final SourceDAO dao = new SourceDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.delete(sourceName);
            }
        });
    }

    public void deleteSource(final String sourceName, final FederationType federationType){
        final SourceDAO dao = new SourceDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.delete(sourceName, federationType);
            }
        });
    }

    public void updateSource(final Source source, final FederationType oldFederationType){
        final SourceDAO dao = new SourceDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.update(source, oldFederationType);
            }
        });
    }

    public void deleteTable(final String table){
        final TableDAO dao = new TableDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.delete(table);
            }
        });
    }

    public void deleteMondrianSchema(final String org, final String ns, final String sourceName){
        final MondrianSchemaDao dao = new MondrianSchemaDao(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Optional<MondrianSchema> mondrianSchemaOptional = dao.getLatest(org, ns, sourceName);
                if(mondrianSchemaOptional.isPresent()) {
                    dao.delete(org, ns, sourceName);
                } else {
                    throw new RuntimeException("No such entity exist");
                }
            }
        });
    }

    public void updateTable(final Table table){
        final TableDAO dao = new TableDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.update(table);
            }
        });
    }

    public Set<String> deleteDimension(final String dimensionName){
        final DimensionDAO dao = new DimensionDAO(sessionFactory);
        final FactDimensionMappingDAO factDimensionMappingDAO = new FactDimensionMappingDAO(sessionFactory);
        final AtomicReference<Set<String>> factSet = new AtomicReference<Set<String>>(Sets.<String>newHashSet());
        final AtomicReference<String> namespace = new AtomicReference<String>();
        final AtomicReference<String> org = new AtomicReference<String>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Set<com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact> facts = factDimensionMappingDAO.getFactsByDimension (dimensionName);
                final com.flipkart.fdp.superbi.cosmos.meta.model.data.Dimension dimension = dao.getDimensionByName(dimensionName);
                namespace.set(dimension.getNamespace().getName());
                org.set(dimension.getNamespace().getOrg().getName());
                Set<String> factNames = Sets.newHashSet(Iterables.transform(facts,new Function<com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact, String>() {
                    @Override
                    public String apply(com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact input) {
                        return input.getName();
                    }
                }));
                dao.delete(dimensionName);
                factSet.set(factNames);
            }
        });
        return factSet.get();
    }

    public Set<String> updateDimension(final Dimension dimension){
        final DimensionDAO dao = new DimensionDAO(sessionFactory);
        final FactDimensionMappingDAO factDimensionMappingDAO = new FactDimensionMappingDAO(sessionFactory);
        final AtomicReference<Set<String>> factSet = new AtomicReference<Set<String>>(Sets.<String>newHashSet());
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Set<com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact> facts = factDimensionMappingDAO.getFactsByDimension (dimension.getName());
                Set<String> factNames = Sets.newHashSet(Iterables.transform(facts,new Function<com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact, String>() {
                    @Override
                    public String apply(com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact input) {
                        return input.getName();
                    }
                }));
                dao.update(dimension);
                factSet.set(factNames);
            }
        });
        return factSet.get();
    }

    public void deleteFact(final String factName){
        final FactDAO dao = new FactDAO(sessionFactory);
        final AtomicReference<String> org = new AtomicReference<String>();
        final AtomicReference<String> namespace = new AtomicReference<String>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                final com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact fact = dao.getFactByName(factName);
                namespace.set(fact.getNamespace().getName());
                org.set(fact.getNamespace().getOrg().getName());
                dao.delete(factName);
            }
        });
    }

    public void updateFact(final Fact fact){
        final FactDAO dao = new FactDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.update(fact);
            }
        });
    }

    public void updateProcessId(final String tableName,final int processId){
        final TableDAO dao = new TableDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.updateProcessId(tableName,processId);
            }
        });
    }

    public void updateSourceForFact(final String factName,final String sourceName){
        final TableDAO dao = new TableDAO(sessionFactory);
        final FactDAO factDao = new FactDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact internalFact = factDao.getFactByName(factName);
                final String tableName = internalFact.getTable().getName();
                dao.updateSource(tableName,sourceName);
            }
        });
    }

    public void updateSourceForDimension(final String dimensionName,final String sourceName){
        final TableDAO dao = new TableDAO(sessionFactory);
        final DimensionDAO dimensionDao = new DimensionDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                com.flipkart.fdp.superbi.cosmos.meta.model.data.Dimension internalDimension = dimensionDao.getDimensionByName(dimensionName);
                final String tableName = internalDimension.getTable().getName();
                dao.updateSource(tableName,sourceName);
            }
        });
    }

    public void updateSourceForTable(final String tableName,final String sourceName){
        final TableDAO dao = new TableDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.updateSource(tableName,sourceName);
            }
        });
    }

    public  void updateBoltLookup(final WebBoltLookup webBoltLookup){
        final BoltLookupDAO dao = new BoltLookupDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.update(webBoltLookup);
            }
        });
    }


    public  void updateBoltSchemaMap(final WebBoltSchemaMap webBoltSchemaMap){
        final BoltSchemaMapDAO dao = new BoltSchemaMapDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.update(webBoltSchemaMap);
            }
        });
    }

    public  void updateBoltPrimaryEntityMap(final WebBoltPrimaryEntityMap webBoltPrimaryEntityMap){
        final BoltPrimaryEntityMapDAO dao = new BoltPrimaryEntityMapDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.update(webBoltPrimaryEntityMap);
            }
        });
    }

    public void deleteBoltSchemaMap(final String factTableName,final String columnName){
        final BoltSchemaMapDAO dao = new BoltSchemaMapDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.delete(factTableName, columnName);
            }
        });
    }

    public void deleteBoltLookup(final String factTableName, final String lookupTableName,final String lookupName){
        final BoltLookupDAO dao = new BoltLookupDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.delete(factTableName,lookupTableName, lookupName);
            }
        });
    }

    public void deleteBoltPrimaryEntityMap(final String factTableName){
        final BoltPrimaryEntityMapDAO dao = new BoltPrimaryEntityMapDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.delete(factTableName);
            }
        });
    }

    public void bulkUpdateFactRefreshTime(Map<String, Date> factRefreshTimeMap) {
        Session session = sessionFactory.openSession();
        Transaction tx = session.beginTransaction();

        Query query = session.createQuery("FROM com.flipkart.fdp.superbi.cosmos.meta.model.data.Table table where table.name IN (:tableNames) and table.deleted = false");
        query.setParameterList("tableNames", factRefreshTimeMap.keySet());
        List tableList = query.list();

//        int count=0;
        for(Object object : tableList) {
            com.flipkart.fdp.superbi.cosmos.meta.model.data.Table table = (com.flipkart.fdp.superbi.cosmos.meta.model.data.Table) object;

            Date newRefreshTime = factRefreshTimeMap.get(table.getName());

            LOGGER.info(String.format("Updating refresh time of fact=%s, old time=%s, new time=%s diffPresent= %s", table.getName(), table.getLastRefresh(), newRefreshTime,
                    table.getLastRefresh() == null ? true : ((newRefreshTime.getTime() - table.getLastRefresh().getTime())/1000)>2));

            table.setLastRefresh(newRefreshTime);


            // TODO if the number increases we may have to batch it also the commented code is buggy
//            if ( ++count % 20 == 0 ) {
//                //flush a batch of updates and release memory:
//            }
        }

        session.flush();
        tx.commit();
        session.close();
    }
}
