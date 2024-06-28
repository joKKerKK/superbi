package com.flipkart.fdp.superbi.cosmos.meta.api;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.fdp.superbi.cosmos.meta.BigzillaConf;
import com.flipkart.fdp.superbi.cosmos.meta.api.client.BigzillaClient;
import com.flipkart.fdp.superbi.cosmos.meta.api.client.DartClient;
import com.flipkart.fdp.superbi.cosmos.meta.db.TransactionLender;
import com.flipkart.fdp.superbi.cosmos.meta.db.WorkUnit;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Attribute;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.DartEntity;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Schema;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.DartIngestionDefRequest;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebDartEntity;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebSchema;
import com.flipkart.fdp.superbi.cosmos.meta.schemautils.SchemaChangeDetector;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.client.HttpClient;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: aartika
 * Date: 4/16/14
 */
public class DartEntityResource {

    private DartEntityDAO dartEntityDAO;
    private SchemaDAO schemaDAO;
    private DartClient dartClient;
    private BigzillaClient bigzillaClient;
    private TransactionLender transactionLender;
    private NamespaceDAO namespaceDAO;
    private CompanyDAO companyDAO;
//    private SchemaOwnerInformationDAO schemaOwnerInformationDAO;
    private TransactionLender lender;
    private static final Logger LOGGER = LoggerFactory.getLogger(DartEntityResource.class);

    public DartEntityResource(SessionFactory sessionFactory,
                              HttpClient httpClient,
                              BigzillaConf bigzillaConf,
                              String dartHost,
                              int dartPort) {
        dartEntityDAO = new DartEntityDAO(sessionFactory);
        schemaDAO = new SchemaDAO(sessionFactory);
        namespaceDAO = new NamespaceDAO(sessionFactory);
        companyDAO = new CompanyDAO(sessionFactory);
        dartClient = new DartClient(httpClient, dartHost, dartPort);
        bigzillaClient = new BigzillaClient(httpClient, bigzillaConf);
        transactionLender = new TransactionLender(sessionFactory);
//        schemaOwnerInformationDAO = new SchemaOwnerInformationDAO(sessionFactory);
        lender = new TransactionLender(sessionFactory);
    }

    public List<WebDartEntity> list() {
        final AtomicReference<List<WebDartEntity>> webDartEntities = new AtomicReference<List<WebDartEntity>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                List<DartEntity> dartEntities = dartEntityDAO.getAll();
                webDartEntities.set(transform(dartEntities));
            }
        });
        return webDartEntities.get();
    }

    public WebDartEntity getByUri(final String company,
                                  final String org,
                                  final String namespace,
                                  final String name) {
        final AtomicReference<WebDartEntity> webDartEntity = new AtomicReference<WebDartEntity>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                DartEntity dartEntity = dartEntityDAO.getByUri(company, org, namespace, name);
                if (dartEntity == null)
                    webDartEntity.set(null);
                else
                    webDartEntity.set(new WebDartEntity(dartEntity));
            }
        });
        return webDartEntity.get();
    }

    public WebDartEntity getByUriAndVersion(final String company,
                                  final String org,
                                  final String namespace,
                                  final String name,
                                  final String version) {
        final AtomicReference<WebDartEntity> webDartEntity = new AtomicReference<WebDartEntity>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                DartEntity dartEntity = dartEntityDAO.getByUriAndVersion(company, org, namespace, name, version);
                if (dartEntity == null)
                    webDartEntity.set(null);
                else
                    webDartEntity.set(new WebDartEntity(dartEntity));
            }
        });
        return webDartEntity.get();
    }


    private List<WebDartEntity> transform(List<DartEntity> dartEntities) {
        return new ArrayList<WebDartEntity>(Collections2.transform(dartEntities,
                new Function<DartEntity, WebDartEntity>() {
                    @Override
                    public WebDartEntity apply(DartEntity input) {
                        return new WebDartEntity(input);
                    }
                }));
    }

    //Register method should be used only for entity/event. Register is not a valid operation for composite_type
    public WebDartEntity register(final WebDartEntity webDartEntity,
                                  final Schema.SchemaType schemaType,
                                  final String company,
                                  final String org,
                                  final String namespace,
                                  final String name,
                                  final Function<WebSchema, String> getDartSchema) {
        final AtomicReference<WebDartEntity> createdWebDartEntity = new AtomicReference<WebDartEntity>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                if (dartEntityDAO.getByUri(company, org, namespace, name) == null) {
                    DartEntity dartEntity = webDartEntity.populate(new DartEntity());
                    Schema schema = schemaDAO.schemaByName(company, org, namespace, name, schemaType);

                    dartEntity.addSchema(schema);

                    String dartEntityVersion;

                    DartIngestionDefRequest dartIngestionDefRequest = new DartIngestionDefRequest(
                            dartEntity, Optional.<String>absent(), getDartSchema);
                    dartEntityVersion=dartClient.createDartEntity(dartIngestionDefRequest);

                    schema.setDartVersion(dartEntityVersion);

                    schemaDAO.save(schema);

                    List<Schema> schemas = schemaDAO.getJiraIdForSchemaApproval(company, org, namespace, name);
                    for(Schema schema1:schemas){
                        if(null!=schema1.getJiraId()){
                            schema1.setJiraId(null);
                            schemaDAO.save(schema1);
                        }
                    }

                    dartEntityDAO.save(dartEntity);
                    createdWebDartEntity.set(new WebDartEntity(dartEntity));
                }
            }
        });
        return createdWebDartEntity.get();
    }

    public SchemaChange getEntityChangeType(final WebDartEntity webDartEntity,
                                        final Schema.SchemaType schemaType,
                                        final String company,
                                        final String org,
                                        final String namespace,
                                        final String name) throws JsonProcessingException{
        final AtomicReference<WebDartEntity> updatedWebDartEntity = new AtomicReference<WebDartEntity>();
        final String changeVersionType[] = new String[1];
        changeVersionType[0] = "unknown";
        final Schema[] latestA = new Schema[1];
        final Schema[] lastRegisteredA = new Schema[1];
        final AtomicReference old  = new AtomicReference();
        final AtomicReference newV  = new AtomicReference();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                DartEntity persisted = dartEntityDAO.getByUri(company, org, namespace, name);
                boolean isMinor;
                if (persisted != null) {
                    DartEntity dartEntity = webDartEntity.populate(persisted);

                    Schema latest = schemaDAO.schemaByName(company, org, namespace, name, schemaType);
                    Schema lastRegistered = schemaDAO.schemaRegistered(company, org, namespace, name, schemaType);

                    Schema tempSchema = lastRegistered;
                    Queue<Schema> queue = new LinkedList<Schema>();
                    do {
                        for (Map.Entry<String, Attribute> entry : tempSchema.getAttributes().entrySet()) {

                            entry.getValue().getAllowedValues().toString();
                            entry.getValue().getValidators().toString();
                            Schema temp = entry.getValue().getTypeSchema();
                            if (temp != null) {
                                queue.add(temp);
                            }
                        }
                        tempSchema = queue.poll();
                    } while (tempSchema != null);

                    tempSchema = latest;
                    queue.clear();
                    do {
                        for (Map.Entry<String, Attribute> entry : tempSchema.getAttributes().entrySet()) {

                            entry.getValue().getAllowedValues().toString();
                            entry.getValue().getValidators().toString();
                            Schema temp = entry.getValue().getTypeSchema();
                            if (temp != null) {
                                queue.add(temp);
                            }
                        }
                        tempSchema = queue.poll();
                    } while (tempSchema != null);
                    latestA[0] = latest;
                    lastRegisteredA[0] = lastRegistered;
                    old.set(new WebSchema(lastRegisteredA[0]));
                    newV.set( new WebSchema(latestA[0]));
                    SchemaChangeDetector detector = new SchemaChangeDetector(lastRegistered, latest);
                    changeVersionType[0] = detector.getUpdateType().toString();

                }
            }
        });
        LOGGER.info("Change type found is {}", changeVersionType[0]);
        SchemaChange obj = new SchemaChange(changeVersionType[0],(WebSchema)old.get(),(WebSchema)newV.get());
        return obj;
        //return new Object(){ public String changeType = changeVersionType[0];};
    }

    public Object getEntityChangeTypeForGivenSchema(final WebSchema webSchema,
                                        final Schema.SchemaType schemaType,
                                        final String company,
                                        final String org,
                                        final String namespace,
                                        final String name) throws JsonProcessingException{
        final String changeVersionType[] = new String[1];
        changeVersionType[0] = "unknown";
        final Schema[] latestA = new Schema[1];
        final Schema[] lastRegisteredA = new Schema[1];
        final AtomicReference old  = new AtomicReference();
        final AtomicReference newV  = new AtomicReference();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                DartEntity persisted = dartEntityDAO.getByUri(company, org, namespace, name);
                if (persisted != null) {
                    latestA[0] = webSchema.populate(new Schema(), namespaceDAO, companyDAO);
                    latestA[0].setSchemaType(schemaType);
                    lastRegisteredA[0] = schemaDAO.schemaRegistered(company, org, namespace, name, schemaType);
                    newV.set( new WebSchema(lastRegisteredA[0]));
                    old.set(new WebSchema(latestA[0]));
                    SchemaChangeDetector detector = new SchemaChangeDetector(lastRegisteredA[0], latestA[0]);
                    changeVersionType[0] = detector.getUpdateType().toString();
                }
            }
        });
        LOGGER.info("Change type found is {}", changeVersionType[0]);
        SchemaChange obj = new SchemaChange(changeVersionType[0],(WebSchema)old.get(),(WebSchema)newV.get());
        return obj;
    }



    public String getEntityChangeTypeBetweenVersions(final String company,
                                      final String org,
                                      final String namespace,
                                      final String name,
                                      final String version1,
                                      final String version2) {
        final String changeVersionType[] = new String[1];
        changeVersionType[0] = "unknown";
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                DartEntity persisted = dartEntityDAO.getByUri(company, org, namespace, name);
                if (persisted != null) {
                    Schema prev  = persisted.getSchema(Optional.of(version1));
                    Schema curr = persisted.getSchema(Optional.of(version2));
                    Schema tempSchema = prev;
                    Queue<Schema> queue = new LinkedList<Schema>();
                    do{
                        for (Map.Entry<String, Attribute> entry : tempSchema.getAttributes().entrySet()) {

                            entry.getValue().getAllowedValues().toString();
                            entry.getValue().getValidators().toString();
                            Schema temp = entry.getValue().getTypeSchema();
                            if( temp != null){
                                queue.add(temp);
                            }
                        }
                        tempSchema = queue.poll();
                    }while(tempSchema != null);

                    tempSchema = curr;
                    queue.clear();
                    do{
                        for (Map.Entry<String, Attribute> entry : tempSchema.getAttributes().entrySet()) {

                            entry.getValue().getAllowedValues().toString();
                            entry.getValue().getValidators().toString();
                            Schema temp = entry.getValue().getTypeSchema();
                            if( temp != null){
                                queue.add(temp);
                            }
                        }
                        tempSchema = queue.poll();
                    }while(tempSchema != null);
                    SchemaChangeDetector detector = new SchemaChangeDetector(prev,curr);
                    changeVersionType[0] = detector.getUpdateType().toString() ;


                }
            }
        });
        LOGGER.info("Change type found is {}",changeVersionType[0]);
        return changeVersionType[0];
    }

    public WebDartEntity update(final WebDartEntity webDartEntity,
                                final Schema.SchemaType schemaType,
                                final String company,
                                final String org,
                                final String namespace,
                                final String name,
                                final UpdateType updateType,
                                final Function<WebSchema, String> getDartSchema) {
        final AtomicReference<WebDartEntity> updatedWebDartEntity = new AtomicReference<WebDartEntity>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                DartEntity persisted = dartEntityDAO.getByUri(company, org, namespace, name);
                boolean isMinor;
                if (persisted != null) {
                    DartEntity dartEntity = webDartEntity.populate(persisted);

                    Schema latest = schemaDAO.schemaByName(company, org, namespace, name, schemaType);

                    dartEntity.addSchema(latest);

                    Schema schema = dartEntity.getSchema(Optional.<String>absent());
                    LOGGER.info("updating dart entity with schema {}", schema);

                    java.util.Optional<Schema> lastRegisteredSchema =
                            dartEntity.getSchemas()
                            .stream()
                            .filter(schema1 -> schema1.getDartVersion() != null)
                            .max(Comparator.comparing(s->s.getId()));

                    String newDartEntityVersion = "";
                    String oldDartEntityVersion = "";

                    if(lastRegisteredSchema.isPresent())
                        oldDartEntityVersion = lastRegisteredSchema.get().getDartVersion();

                    if(updateType == UpdateType.ingestion_source) {
                        changeIngestionStatusForSource(company, org, namespace, name, dartEntity.getSchema(Optional.<String>absent()).getIngestionSource() , StatusOp.activate);
                    }
                    else {
                        DartIngestionDefRequest dartIngestionDefRequest =
                                new DartIngestionDefRequest(dartEntity, Optional.<String>absent(), getDartSchema);
                        newDartEntityVersion = dartClient.updateDartEntity(dartIngestionDefRequest, updateType);
                    }

                    if (updateType == UpdateType.semantics || updateType == UpdateType.ingestion_source)
                        schema.setDartVersion(oldDartEntityVersion);
                     else
                        schema.setDartVersion(newDartEntityVersion);

                    schemaDAO.save(schema);
                    List<Schema> schemas = schemaDAO.getJiraIdForSchemaApproval(company, org, namespace, name);
                    for(Schema schema1:schemas){
                        if(null!=schema1.getJiraId()){
                            schema1.setJiraId(null);
                            schemaDAO.save(schema1);
                        }
                    }
                    dartEntityDAO.save(dartEntity);
                    WebDartEntity webDartEntity = new WebDartEntity(dartEntity);
                    webDartEntity.setSchemaUpdateType(updateType);
                    updatedWebDartEntity.set(webDartEntity);
                }
            }
        });
        return updatedWebDartEntity.get();
    }




    public WebDartEntity forceUpdate(final WebDartEntity webDartEntity,
                                final Schema.SchemaType schemaType,
                                final String company,
                                final String org,
                                final String namespace,
                                final String name,
                                final Function<WebSchema, String> getDartSchema,
                                final String changeType) {

        // this case will never happen so we are not doing anything... We use force update only when we are doing major version change fro the same schema
        if(changeType.equalsIgnoreCase("ingestion_source")) return webDartEntity ;
        final AtomicReference<WebDartEntity> updatedWebDartEntity = new AtomicReference<WebDartEntity>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                DartEntity persisted = dartEntityDAO.getByUri(company, org, namespace, name);
                boolean isMinor;
                if (persisted != null) {
                    DartEntity dartEntity = webDartEntity.populate(persisted);

                    Schema latest = schemaDAO.schemaByName(company, org, namespace, name, schemaType);
                    Schema lastRegistered = schemaDAO.schemaRegistered(company, org, namespace, name, schemaType);

                    Schema tempSchema = lastRegistered;
                    Queue<Schema> queue = new LinkedList<Schema>();

                    // What is below code doing ??
                    do{
                        for (Map.Entry<String, Attribute> entry : tempSchema.getAttributes().entrySet()) {

                            // entry.getValue().getAllowedValues().toString();
                            //entry.getValue().getValidators().toString();
                            Schema temp = entry.getValue().getTypeSchema();
                            if( temp != null){
                                queue.add(temp);
                            }
                        }
                        tempSchema = queue.poll();
                    }while(tempSchema != null);

                    tempSchema = latest;
                    queue.clear();
                    do{
                        for (Map.Entry<String, Attribute> entry : tempSchema.getAttributes().entrySet()) {

                            // entry.getValue().getAllowedValues().toString();
                            // entry.getValue().getValidators().toString();
                            Schema temp = entry.getValue().getTypeSchema();
                            if( temp != null){
                                queue.add(temp);
                            }
                        }
                        tempSchema = queue.poll();
                    }while(tempSchema != null);

//                    SchemaChangeDetector detector = new SchemaChangeDetector(lastRegistered,latest);
//                    ChangeType.BroadChangeType schemaChangeType = detector.getUpdateType();
//                    UpdateType updateType = UpdateType.semantics;
//                    switch (schemaChangeType) {
//                        case MAJOR: updateType = UpdateType.major;
//                            break;
//                        case MINOR: updateType = updateType.minor;
//                            break;
//                        case NONE:  updateType = updateType.semantics;
//                            break;
//                    }
                    dartEntity.addSchema(latest);

                    Schema schema = dartEntity.getSchema(Optional.<String>absent());
                    LOGGER.info("updating dart entity with schema {}", schema);
                    DartIngestionDefRequest dartIngestionDefRequest =
                            new DartIngestionDefRequest(dartEntity, Optional.<String>absent(), getDartSchema);
                    UpdateType updateType= UpdateType.semantics;
                    for(DartEntityResource.UpdateType u: DartEntityResource.UpdateType.values())
                    {
                        if(u.toString().equals(changeType.toLowerCase()))
                        {
                            updateType=u;
                        }
                    }
                    String dartEntityVersion = dartClient.updateDartEntity(dartIngestionDefRequest, updateType);
                    if (updateType != UpdateType.semantics) {
                        schema.setDartVersion(dartEntityVersion);
                        schemaDAO.save(schema);
                    }
                    dartEntityDAO.save(dartEntity);
                    WebDartEntity webDartEntity = new WebDartEntity(dartEntity);
                    webDartEntity.setSchemaUpdateType(updateType);
                    updatedWebDartEntity.set(webDartEntity);
                }
            }
        });
        return updatedWebDartEntity.get();
    }


    public Map<String, RegistrationStatus> getRegistrationStatus() {
        final AtomicReference<Map<String, RegistrationStatus>> statusMapRef =
                new AtomicReference<Map<String, RegistrationStatus>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Map<String, DartEntity> dartEntityMap = new HashMap<String, DartEntity>();
                List<DartEntity> dartEntities = dartEntityDAO.getAll();
                for (DartEntity dartEntity : dartEntities) {
                    dartEntityMap.put(dartEntity.getUri(), dartEntity);
                }
                List<Schema> schemas = schemaDAO.getAllEntities();
                schemas.addAll(schemaDAO.getAllEvents());
                Map<String, RegistrationStatus> statusMap = new HashMap<String, RegistrationStatus>();
                for (Schema schema : schemas) {
                    String uri = String.format("%s/%s/%s",
                            schema.getNamespace().getOrg().getName(),
                            schema.getNamespace().getName(),
                            schema.getName());
                    if (!dartEntityMap.containsKey(uri)) {
                        statusMap.put(uri, RegistrationStatus.unregistered);
                    } else {
                        DartEntity registered = dartEntityMap.get(uri);
                        if (registered.getSchema(Optional.<String>absent()).getId().equals(schema.getId()))
                            statusMap.put(uri, RegistrationStatus.latest);
                        else
                            statusMap.put(uri, RegistrationStatus.stale);
                    }
                }
                statusMapRef.set(statusMap);
            }
        });

        return statusMapRef.get();
    }

    public Map<String, DartClient.Status> getAllDartStatus() {
        final AtomicReference<Map<String, DartClient.Status>> statusMap =
                new AtomicReference<Map<String, DartClient.Status>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                statusMap.set(dartClient.getAllDartStatus());
            }
        });
        return statusMap.get();
    }

    public JsonNode getAllDartStatusNew() {
        return dartClient.getAllDartStatusNew();
    }

    public JsonNode getDartStatus(String company, String org, String namespace, String name) {
        return dartClient.getDartStatus(company, org, namespace, name);
    }

    public Map<String, DartClient.UrlsBean> getAllMetricUrlsModified() {
        return dartClient.getAllMetricsUrlsModified();
    }

    public JsonNode getAllErrorCount() {
        return bigzillaClient.getErrorCountForDashboard();
    }

    public JsonNode getErroredIngestion(String nameSubstring, int offset) {
        checkArgument(offset >= 0, "Offset has to be non-negative");
        return bigzillaClient.getErroredIngestion(nameSubstring, offset);
    }

    public JsonNode getErrorSummary() {
        return bigzillaClient.getAllErrorSummary();
    }

    public JsonNode getErrorSummary(String company, String org, String namespace, String name) {
        return bigzillaClient.getErrorSummary(company, org, namespace, name);
    }

    public JsonNode bootstrap(String company, String org, String namespace, String name) {
        return dartClient.bootstrap(company, org, namespace, name);
    }

    public JsonNode changeIngestionStatusForSource(String company, String org, String namespace, String name, String source, StatusOp status) {
        return dartClient.changeSourceStatus(company, org, namespace, name, source , status.toString() );
    }

    public JsonNode getDartboardMetrics() {
        return dartClient.getDartboardMetrics();
    }

    public JsonNode getDartboardMetricsHistory(final String company,
                                               final String org,
                                               final String namespace,
                                               final String name,
                                               final String start,
                                               final Optional<String> end) {
        return dartClient.getDartboardMetricsHistory(company, org, namespace, name, start, end);
    }

    public JsonNode getDartboardMetricsHistoryForNamespace(String company,
                                                           String org,
                                                           String namespace,
                                                           String start,
                                                           Optional<String> end) {
        return dartClient.getDartboardMetricsHistoryForNamespace(company, org, namespace, start, end);
    }

    public enum UpdateType {
        major,
        minor,
        semantics,
        ingestion_source;

        public String toString(){
            return this.name().toLowerCase();
        }

    }

    public enum RegistrationStatus {
        latest,
        stale,
        unregistered
    }
    private void doInTransaction(WorkUnit work) {
        this.lender.execute(work);
    }

    public enum StatusOp {
        activate, suspend;
    }

}
