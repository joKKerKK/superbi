package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.api.client.JiraClient;
import com.flipkart.fdp.superbi.cosmos.meta.db.TransactionLender;
import com.flipkart.fdp.superbi.cosmos.meta.db.WorkUnit;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.CompanyDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.NamespaceDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.SchemaDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Attribute;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Schema;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SchemaComponent;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.DartRegistrationStatus;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.SchemaInfo;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebSchema;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import net.rcarz.jiraclient.JiraException;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: aartika
 * Date: 3/23/14
 */
public class SchemaResource {

    private SchemaDAO schemaDAO;
    private NamespaceDAO namespaceDAO;
    private CompanyDAO companyDAO;
    private TransactionLender transactionLender;
    private Schema.SchemaType schemaType;
    private static String[] uriMapKeys = {"company.name","org.name","namespace.name","name"};
    private JiraClient jiraClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaResource.class);

    public SchemaResource(SessionFactory sessionFactory, Schema.SchemaType schemaType) {
        this.schemaType = schemaType;
        schemaDAO = new SchemaDAO(sessionFactory);
        namespaceDAO = new NamespaceDAO(sessionFactory);
        companyDAO = new CompanyDAO(sessionFactory);
        transactionLender = new TransactionLender(sessionFactory);
        jiraClient = new JiraClient();
    }
@Deprecated
    public List<SchemaInfo> listInfo() {
       return listInfo("%", "%", 0, 0);
    }

    /**
     * full text search on event and entity
     * the function is only supported with mysql 5.6 or  higher.
     */
    public List<SchemaInfo> listInfo(String searchKey,Integer offset,Integer limit) {
        final AtomicReference<List<SchemaInfo>> schemaInfos = new AtomicReference<List<SchemaInfo>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                schemaInfos.set(Lists.newArrayList(Collections2.transform(schemaDAO.getSchemasWithFullTextSearch(searchKey, offset, limit), new Function<Schema, SchemaInfo>() {
                    @Override
                    public SchemaInfo apply(Schema schema) {
                        return new SchemaInfo(schema);
                    }
                })));
            }
        });
        return schemaInfos.get();
    }

    public List<SchemaInfo> listSchemaInfo(String company, String org, String namespace, String nameFilter, Integer offset, Integer limit) {
        final AtomicReference<List<SchemaInfo>> schemaInfos = new AtomicReference<List<SchemaInfo>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                schemaInfos.set(Lists.newArrayList(Collections2.transform(schemaDAO.getSchemasInfo(company, org, namespace, nameFilter, schemaType, offset, limit), new Function<Schema, SchemaInfo>() {
                    @Override
                    public SchemaInfo apply(Schema schema) {
                        return new SchemaInfo(schema);
                    }
                })));
            }
        });
        return schemaInfos.get();
    }

    public List<WebSchema> listCustomTypeInfo(String company, String org, String namespace, String nameFilter, Integer offset, Integer limit) {
        final AtomicReference<List<WebSchema>> webSchemaList = new AtomicReference<List<WebSchema>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                List<Schema> schemaList = schemaDAO.getCompositeTypeInfo(company, org, namespace, nameFilter, offset, limit);
                webSchemaList.set(new ArrayList<WebSchema>(Collections2.transform(schemaList, new Function<Schema, WebSchema>() {
                    @Override
                    public WebSchema apply(Schema input) {
                        return new WebSchema(input);
                    }
                })));
            }
        });
        return webSchemaList.get();
    }

    /**
     * search on event and entity
     * it will perform SQL regex (like clause) matching.
     */
    public List<SchemaInfo> listInfo(String descPattern, String namePattern,Integer offset, Integer limit) {
        final AtomicReference<List<SchemaInfo>> schemaInfos = new AtomicReference<List<SchemaInfo>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                schemaInfos.set(Lists.newArrayList(Collections2.transform(schemaDAO.getSchemas(schemaType, descPattern, namePattern, offset, limit), new Function<Schema, SchemaInfo>() {
                    @Override
                    public SchemaInfo apply(Schema schema) {
                        return new SchemaInfo(schema);
                    }
                })));
            }
        });
        return schemaInfos.get();
    }


    public List<WebSchema> list(Optional<String> company,
            Optional<String> org,
            Optional<String> namespace,
            Optional<SchemaComponent.Level> level,
            Optional<Boolean> deleted)  {
        final Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put("schemaType", schemaType);
        if (company.isPresent())
            paramMap.put("company.name", company.get());
        if(org.isPresent())
            paramMap.put("org.name", org.get());
        if(namespace.isPresent())
            paramMap.put("namespace.name", namespace.get());
        if(level.isPresent())
            paramMap.put("level", level.get());
        if (deleted.isPresent())
            paramMap.put("deleted", deleted.get());
        else
            paramMap.put("deleted", false);

        final AtomicReference<List<WebSchema>> webSchemaList = new AtomicReference<List<WebSchema>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                List<Schema> schemaList = schemaDAO.getWithFilters(paramMap);
                webSchemaList.set(new ArrayList<WebSchema>(Collections2.transform(schemaList, new Function<Schema, WebSchema>() {
                    @Override
                    public WebSchema apply(Schema input) {
                        return new WebSchema(input);
                    }
                })));
            }
        });
        return webSchemaList.get();
    }

    public List<DartRegistrationStatus> listDartRegistrationStatus(String uri) {
        Iterable<String> splittedUri = Splitter.on('/')
                .trimResults()
                .omitEmptyStrings()
                .split(uri);
        if(!splittedUri.iterator().hasNext())
        {
            final AtomicReference<List<DartRegistrationStatus>> dartRegistrationStatusList = new AtomicReference<List<DartRegistrationStatus>>();
            transactionLender.execute(new WorkUnit() {
                @Override
                public void actualWork() {
                    List<Schema> schemaList = schemaDAO.getAllNonCompositeSchemas();
                    dartRegistrationStatusList.set(new ArrayList<DartRegistrationStatus>(Collections2.transform(schemaList, new Function<Schema, DartRegistrationStatus>() {
                        @Override
                        public DartRegistrationStatus apply(Schema input) {
                            return new DartRegistrationStatus(input);
                        }
                    })));
                }
            });
            return dartRegistrationStatusList.get();
        }
        final Map<String,Object> uriMap = new HashMap<String,Object>();
        final Iterator<String> iterator = splittedUri.iterator();
        for(int i=0; iterator.hasNext();i++) {
            uriMap.put(uriMapKeys[i], iterator.next());
        }
        final AtomicReference<List<DartRegistrationStatus>> dartRegistrationStatusList = new AtomicReference<List<DartRegistrationStatus>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                List<Schema> schemaList = schemaDAO.getNonCompositeSchemasWithFilters(uriMap);
                dartRegistrationStatusList.set(new ArrayList<DartRegistrationStatus>(Collections2.transform(schemaList, new Function<Schema, DartRegistrationStatus>() {
                    @Override
                    public DartRegistrationStatus apply(Schema input) {
                        return new DartRegistrationStatus(input);
                    }
                })));
            }
        });
        return  dartRegistrationStatusList.get();
    }

    public WebSchema getById(final String company,
                             final String org,
                             final String namespace,
                             final String name) {
        final AtomicReference<WebSchema> webSchema = new AtomicReference<WebSchema>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Schema schema = schemaDAO.schemaByName(company, org, namespace, name, schemaType);
                if (schema == null)
                    webSchema.set(null);
                else {
                    schema.setJiraId(getJiraIdIfexist(company, org, namespace, name));
                    webSchema.set(new WebSchema(schema));
                }
            }
        });
        return webSchema.get();
    }

    public WebSchema getByIdAndVersion(final String company, final String org,
                                       final  String namespace, final String name, final int schemaVersion) {
        final AtomicReference<WebSchema> webSchema = new AtomicReference<WebSchema>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Schema schema = schemaDAO.schemaByNameAndVersion(company, org, namespace, name, schemaType, schemaVersion);
                if (schema == null)
                    webSchema.set(null);
                else
                    webSchema.set(new WebSchema(schema));
            }
        });
        return webSchema.get();
    }

    public WebSchema create(final WebSchema webSchema) {
        final AtomicReference<WebSchema> createdWebSchema = new AtomicReference<WebSchema>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Schema persisted = schemaDAO.schemaByName(webSchema.getCompany(), webSchema.getOrg(),
                        webSchema.getNamespace(),
                        webSchema.getName(),
                        schemaType);
                if (persisted == null) {
                    Schema schema = webSchema.populate(new Schema(), namespaceDAO, companyDAO);
                    schema.setSchemaType(schemaType);
                    schema.setSchemaVersion(schemaDAO.getNextVersion(webSchema.getCompany(), webSchema.getOrg(),
                            webSchema.getNamespace(),
                            webSchema.getName(),
                            schemaType));
                    for (Map.Entry<String, Attribute> attributeEntry: schema.getAttributes().entrySet()) {
                        Attribute attribute = attributeEntry.getValue();
                        String[] split = attribute.getType().split("/");
                        if (split.length == 4) {
                            Schema typeSchema = schemaDAO.getTypeByName(split[0], split[1], split[2], split[3]);
                            attribute.setTypeSchema(typeSchema);
                        }
                    }
                    Schema created = schemaDAO.save(schema);
                    createdWebSchema.set(new WebSchema(created));
                }
            }
        });

        return createdWebSchema.get();
    }

    public WebSchema update(final WebSchema webSchema){
        final AtomicReference<WebSchema> updatedWebSchema = new AtomicReference<WebSchema>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Schema persisted = schemaDAO.schemaByName(webSchema.getCompany(), webSchema.getOrg(),
                        webSchema.getNamespace(),
                        webSchema.getName(),
                        schemaType);
                if (persisted != null) {
                    LOGGER.info("updating schema: {} \n to {}", persisted, webSchema);
                    Schema schema = webSchema.populate(new Schema(), namespaceDAO, companyDAO);
                    schema.setSchemaType(schemaType);
                    schema.setSchemaVersion(persisted.getSchemaVersion() + 1);
                    for (Map.Entry<String, Attribute> attributeEntry: schema.getAttributes().entrySet()) {
                        Attribute attribute = attributeEntry.getValue();
                        String[] split = attribute.getType().split("/");
                        if (split.length == 4) {
                            Schema typeSchema = schemaDAO.getTypeByName(split[0], split[1], split[2], split[3]);
                            attribute.setTypeSchema(typeSchema);
                        }
                    }
                    schemaDAO.deleteEntity(persisted);
                    Schema updated = schemaDAO.save(schema);
                    updatedWebSchema.set(new WebSchema(updated));
                }
            }
        });

        return updatedWebSchema.get();
    }

    public void deleteById(final String company,
                           final String org,
                           final String namespace,
                           final String name) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Schema schema = schemaDAO.schemaByName(company, org, namespace, name, schemaType);
                if (schema != null) {
                    LOGGER.info("deleting schema: {}", schema);
                    schemaDAO.deleteEntity(schema);
                }
            }
        });
    }

    public List<String> getSchemaEngOwners(String company, String org, String namespace, String name){
        List<String> lst = new ArrayList<String>(1);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                lst.add(schemaDAO.getEngOwnerCSV(company, org, namespace, name, schemaType));
            }
        });
        return new ArrayList<>(Arrays.asList(lst.get(0).split(",")));
    }

    public List<String> getSchemaProdOwners(String company, String org, String namespace, String name){
        List<String> lst = new ArrayList<String>(1);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                lst.add(schemaDAO.getProdOwnerCSV(company, org, namespace, name, schemaType));
            }
        });
        return new ArrayList<>(Arrays.asList(lst.get(0).split(",")));
    }

    public List<String> getSchemaProdOwnersByNamespace(String company, String org, String namespace) {
        List<String> lst = new ArrayList<String>(1);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                lst.add(schemaDAO.getProdOwnersByNamespace(company, org, namespace));
            }
        });
        return new ArrayList<>(Arrays.asList(lst.get(0).split(",")));
    }

    public List<String> getSchemaEngOwnersByNamespace(String company, String org, String namespace) {
        List<String> lst = new ArrayList<String>(1);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                lst.add(schemaDAO.getEngOwnersByNamespace(company, org, namespace));
            }
        });
        return new ArrayList<>(Arrays.asList(lst.get(0).split(",")));
    }

    public String getJiraIdForSchemaApproval(String company, String org, String namespace, String name, HashMap<String,String> jiraParams) throws IOException, JiraException {

        String jiraId = getJiraIdIfexist(company, org, namespace, name);
        if (jiraId != null && !jiraId.isEmpty()){
            return jiraId;
        }
        else {
            jiraId = jiraClient.createNewTicket(jiraParams);
            final String finalJiraId = jiraId;
            transactionLender.execute(new WorkUnit() {
                @Override
                public void actualWork() {
                    Schema schema = schemaDAO.getSchemaByName(company, org, namespace, name).get(0);
                    schema.setJiraId(finalJiraId);
                    schemaDAO.update(schema);

                }
            });
        }
        return jiraId;
    }

    private String getJiraIdIfexist(String company, String org, String namespace, String name) {
        List <Schema> schemas = new ArrayList<>();
        String jiraId = null;
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                schemas.addAll(schemaDAO.getJiraIdForSchemaApproval(company, org, namespace, name));
            }
        });
        if(!schemas.isEmpty()) {
            jiraId = schemas.get(0).getJiraId();
        }
        return jiraId;
    }
}
