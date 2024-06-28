package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.db.TransactionLender;
import com.flipkart.fdp.superbi.cosmos.meta.db.WorkUnit;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Process;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact;
import java.util.Map;
import java.util.logging.Logger;
import org.hibernate.SessionFactory;

/**
 * User: aniruddha.gangopadhyay
 * Date: 24/02/14
 * Time: 3:01 PM
 */
public class MetaCreator {

    private SessionFactory sessionFactory;
    private TransactionLender transactionLender;
    private CosmosQueryAuditer auditer;

    public MetaCreator(SessionFactory sessionFactory){
        this.sessionFactory = sessionFactory;
        transactionLender = new TransactionLender(sessionFactory);
    }

    private static MetaCreator defaultInstance;
    // Havent made this as a singleton as the dependencies are yet to be figured out

    public static void initialize(SessionFactory sessionFactory) {
        defaultInstance = new MetaCreator(sessionFactory);
        defaultInstance.deleteOldProcessList();
    }

    /**
     * TODO This should ideally go into constructor. Injecting via setter now so that there are less downstream changes
     * @param auditer
     */
    public void setAuditer(CosmosQueryAuditer auditer) {
        if(this.auditer == null) {
            this.auditer = auditer;
        }
    }

    private void deleteOldProcessList() {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                try {
                    new ProcessDAO(sessionFactory).deleteAllBy(java.net.InetAddress.getLocalHost().getHostName());
                } catch (Exception e) {
                    Logger.getLogger("MetaCreator").warning("Cannot delete old");
                }
            }
        });

    }

    public static MetaCreator get() {
        if(defaultInstance == null) {
            throw new RuntimeException("Meta creator is not initialized!");
        }
        return defaultInstance;
    }

    /*
    *
    * Current functionality of all create APIs does not support updates in schema,
    * basic insert with the initial check if entity with the same name is present or not
    *
    */
    public void createOrg(final String orgName){
        final OrgDAO dao = new OrgDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.save(orgName);
            }
        });
    }

    public void addFactProperties(String factName, Map<String,String> properties, boolean isActive){
        final TablePropertiesDao dao = new TablePropertiesDao(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.saveOrUpdate(factName,properties,isActive);
            }
        });
    }

    public void createNamespace(final View.Namespace namespace){
        final NamespaceDAO dao = new NamespaceDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.save(namespace);
            }
        });
    }

    public void createCube(final EMondrianSchema eMondrianSchema) {
        final MondrianSchemaDao dao = new MondrianSchemaDao(sessionFactory);
        final SourceDAO sourceDao = new SourceDAO(sessionFactory);
        final NamespaceDAO nsDao = new NamespaceDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.save(
                        new MondrianSchema (
                            nsDao.getNamespaceByOrgAndName(eMondrianSchema.org, eMondrianSchema.ns),
                            eMondrianSchema.schemaName,
                            eMondrianSchema.description,
                            eMondrianSchema.mondrianSchemaXML,
                            sourceDao.getSourceByName(eMondrianSchema.source)
                        )
                );
            }
        });
    }

//    void createSourceType(final String sourceTypeName){
//        final SourceTypeDAO sourceTypeDAO = new SourceTypeDAO(sessionFactory);
//        transactionLender.execute(new WorkUnit() {
//            @Override
//            public void actualWork() {
//                sourceTypeDAO.save(sourceTypeName);
//            }
//        });
//    }

    public void createSource(final com.flipkart.fdp.superbi.cosmos.meta.model.external.Source source){
        final SourceDAO sourceDAO = new SourceDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                sourceDAO.save(source);
            }
        });
    }

    public void createTable(final com.flipkart.fdp.superbi.cosmos.meta.model.external.Table table){
        final TableDAO tableDAO = new TableDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                tableDAO.save(table);
            }
        });
    }

    public void createDimension(final Dimension dimension) {
        final DimensionDAO dimensionDAO= new DimensionDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dimensionDAO.save(dimension);
            }
        });
    }

    public void createFact(final Fact fact) {
        final FactDAO factDAO = new FactDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                factDAO.save(fact);
            }
        });
    }

    public void createBoltLookup(final WebBoltLookup webBoltLookup){
        final BoltLookupDAO boltLookupDAO = new BoltLookupDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                boltLookupDAO.save(webBoltLookup);
            }
        });
    }

    public void createBoltSchemaMap(final WebBoltSchemaMap webBoltSchemaMap){
        final BoltSchemaMapDAO boltSchemaMapDAO= new BoltSchemaMapDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                boltSchemaMapDAO.save(webBoltSchemaMap);
            }
        });
    }

    public void createBoltPrimaryEntityMap(final WebBoltPrimaryEntityMap webBoltPrimaryEntityMap){
        final BoltPrimaryEntityMapDAO boltPrimaryEntityMapDAO = new BoltPrimaryEntityMapDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                boltPrimaryEntityMapDAO.save(webBoltPrimaryEntityMap);
            }
        });
    }

    public void logStartProcess(final Process process) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                new ProcessDAO(sessionFactory).save(process);
            }
        });
    }

    public void logStopProcess(final Process process) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                new ProcessDAO(sessionFactory).delete(process);
            }
        });
    }

    public void createTag(final String tagName, final String user, final String dataSourceName, final TagAssociation.SchemaType schemaType){
        final TagDAO dao = new TagDAO(sessionFactory);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dao.save(tagName, user, dataSourceName, schemaType);
            }
        });
    }

    public void logExecutorQueryInfo(ExecutorQueryInfoLog executorQueryInfoLog) {
        auditer.audit(executorQueryInfoLog);
    }
}
