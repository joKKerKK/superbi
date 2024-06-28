package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.db.TransactionLender;
import com.flipkart.fdp.superbi.cosmos.meta.db.WorkUnit;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.VerticaProjectionColumnsDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.VerticaProjectionsTableDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.VerticaTableColumnsDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.VerticaProjectionColumns;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.VerticaProjectionsTable;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.VerticaTableColumn;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.VerticaTableOrProjection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.hibernate.SessionFactory;

/**
 * Created by chandrasekhar.v on 11/08/15.
 */
public class VerticaMetaAccessor {
    private final SessionFactory sessionFactory;
    private final TransactionLender transactionLender;

    private static VerticaMetaAccessor defaultInstance;
    // Havent made this as a singleton as the dependencies are yet to be figured out

    public static void initialize(SessionFactory sessionFactory) {
        if(defaultInstance == null)
            defaultInstance = new VerticaMetaAccessor(sessionFactory);
    }

    public static VerticaMetaAccessor get() {
        if(defaultInstance == null) {
            throw new RuntimeException("Meta accessor is not initialized!");
        }
        return defaultInstance;
    }

    public VerticaMetaAccessor(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        this.transactionLender = new TransactionLender(sessionFactory);
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public List<VerticaTableOrProjection> getProjections (String tableName) {

        VerticaProjectionsTableDAO projectionsDAO = new VerticaProjectionsTableDAO(this.sessionFactory);
        VerticaProjectionColumnsDAO projectionColumnsDAO = new VerticaProjectionColumnsDAO(this.sessionFactory);
        final AtomicReference<List<VerticaTableOrProjection>> ProjectionsReference = new AtomicReference<>();

        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {

                List<VerticaTableOrProjection> projections = new ArrayList<VerticaTableOrProjection>();

                List<VerticaProjectionsTable> verticaProjectionsTables = projectionsDAO.getProjectionTables(tableName);
                Map<Long,List<VerticaProjectionsTable>> projectionTablesGroupedByProjectionId = verticaProjectionsTables.stream().collect(Collectors.groupingBy(VerticaProjectionsTable::getProjectionId));
                
                List<Long> projectionIds = new ArrayList<Long>();
                projectionIds.addAll(projectionTablesGroupedByProjectionId.keySet());

                List<VerticaProjectionColumns> projectionColumns = projectionColumnsDAO.getProjectionColumns(projectionIds);
                Map<Long, List<VerticaProjectionColumns>> map = projectionColumns.stream().collect(Collectors.groupingBy(VerticaProjectionColumns::getProjectionId));

                for (Long projectionId : projectionIds) {

                    List<VerticaProjectionsTable> verticaProjectionsTableList = projectionTablesGroupedByProjectionId.get(projectionId);
                    VerticaProjectionsTable verticaProjectionsTable = verticaProjectionsTableList.get(0);

                    List<VerticaTableOrProjection.Column> columns = new ArrayList<>();
                    HashMap<Integer, String> orderByPositions = new HashMap<>();

                    List<VerticaProjectionColumns> projectionColumnsList = map.get(projectionId);

                    for (VerticaProjectionColumns projectionColumn : projectionColumnsList) {
                        VerticaTableOrProjection.Column column = new VerticaTableOrProjection.Column(projectionColumn.getTableColumnName(),
                                projectionColumn.getDataType(), null, projectionColumn.getEncodingType());
                        columns.add(column);
                        if (projectionColumn.getSortPosition() != null)
                            orderByPositions.put(projectionColumn.getSortPosition(),
                                    verticaProjectionsTable.getAnchorTableName().concat(".").concat(projectionColumn.getTableColumnName()));
                    }
                    List<String> orderByColumns = new ArrayList<>(orderByPositions.values());

                    VerticaTableOrProjection projection = new VerticaTableOrProjection(VerticaTableOrProjection.CreateType.PROJECTION, verticaProjectionsTable.getProjectionBaseName(), columns,
                            verticaProjectionsTable.getAnchorTableName(), orderByColumns, verticaProjectionsTable.getSegmentationExpression(), verticaProjectionsTable.getkSafe(), 0);

                    projections.add(projection);
                }

                ProjectionsReference.set(projections);
            }
        });

        return ProjectionsReference.get();
    }

    public VerticaTableOrProjection getTableMetaDefinition(String tableName) {
        VerticaTableColumnsDAO verticaTableColumnsDAO = new VerticaTableColumnsDAO(this.sessionFactory);
        final AtomicReference<VerticaTableOrProjection> ProjectionsReference = new AtomicReference<>();

        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {

                List<VerticaTableColumn> verticaTableColumns = verticaTableColumnsDAO.getVerticaTableColumns(tableName);

                List<VerticaTableOrProjection.Column> columns = new ArrayList<VerticaTableOrProjection.Column>();
                for (VerticaTableColumn verticaTableColumn : verticaTableColumns) {
                    VerticaTableOrProjection.Column column = new VerticaTableOrProjection.Column(verticaTableColumn.getColumnName(), verticaTableColumn.getDataType(), null, null);
                    columns.add(column);
                }

                VerticaTableOrProjection tableMetaDefintion = new VerticaTableOrProjection(VerticaTableOrProjection.CreateType.TABLE, tableName, columns, tableName, null, null, null, 0);
                ProjectionsReference.set(tableMetaDefintion);
            }
        });

        return ProjectionsReference.get();
    }
}
