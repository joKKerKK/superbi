package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.db.TransactionLender;
import com.flipkart.fdp.superbi.cosmos.meta.db.WorkUnit;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.BoltLookupDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.BoltPrimaryEntityMapDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.BoltSchemaMapDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.DataSourceDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.DegenerateDimensionDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.DimensionDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.ExecutorQueryInfoDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.FactDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.FactDimensionMappingDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.MondrianSchemaDao;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.NamespaceDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.NamespaceDomainMappingDao;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.OrgDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.SourceDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.SourceTypeDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.TableDAO;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.TablePropertiesDao;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.TagDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.BoltLookup;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.BoltPrimaryEntityMap;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.BoltSchemaMap;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.DataSource;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.DegenerateDimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.FactDimensionMapping;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Hierarchy;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Level;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.MondrianSchema;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Namespace;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.NamespaceDomainMapping;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Org;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Source;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Table;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.TableProperties;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.TagAssociation;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.EMondrianSchema;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.JasperDomain;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.View;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebBoltLookup;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebBoltPrimaryEntityMap;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebBoltSchemaMap;
import com.flipkart.fdp.superbi.cosmos.meta.util.MapUtil;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;

/**
 * User: aniruddha.gangopadhyay
 * Date: 28/02/14
 * Time: 12:54 AM
 */
@Slf4j
public class MetaAccessor {
  private final SessionFactory sessionFactory;
  private final TransactionLender transactionLender;

  private static MetaAccessor defaultInstance;
  // Havent made this as a singleton as the dependencies are yet to be figured out

  private  LoadingCache<String,Map<String,String>> tablePropertiesLoadingCache = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build(
      new CacheLoader<String, Map<String,String>>() {
        @Override
        public Map<String,String> load(String s)  {
          return getTablePropertiesByTableName(s);
        }
      });

  public static void initialize(SessionFactory sessionFactory) {
    if(defaultInstance == null)
      defaultInstance = new MetaAccessor(sessionFactory);
  }

  public static MetaAccessor get() {
    if(defaultInstance == null) {
      throw new RuntimeException("Meta accessor is not initialized!");
    }
    return defaultInstance;
  }

  /*-----------------------helper attributes---------------------------------*/
  private final Function<Dimension,View.Entity> dimTransform = new Function<Dimension, View.Entity>() {
    @Override
    public View.Entity apply(Dimension dimension) {
      return new View.DimensionEntity(dimension.getName(),
          View.Entity.ProcessPipeline.valueOf(dimension.getTable().getProcessPipeline()),
          dimension.getType(),dimension.getDescription(),dimension.getTable().getName(),dimension.getTable().getProcessId());
    }
  };

  private final Function<Dimension, View.Entity> dimTransformIncludeNamespace =
      new Function<Dimension, View.Entity>() {
        @Override
        public View.Entity apply(Dimension dimension) {
          return new View.DimensionEntity(dimension.getName(),
              View.Entity.ProcessPipeline.valueOf(dimension.getTable().getProcessPipeline()),
              dimension.getType(),
              dimension.getDescription(),
              dimension.getTable().getName(),
              dimension.getTable().getProcessId(),
              new View.Namespace(dimension.getNamespace())
          );
        }
      };

  private final Function<DegenerateDimension, View.DimensionEntity> degenerateDimensionTransform = new Function<DegenerateDimension, View.DimensionEntity>() {
    @Override
    public View.DimensionEntity apply(com.flipkart.fdp.superbi.cosmos.meta.model.data.DegenerateDimension degenerateDimension) {
      return new View.DimensionEntity(degenerateDimension.getName(), null, DataSource.Type.dimension, degenerateDimension.getDescription(),true);
    }
  } ;

  private final Function<Fact,View.Entity> factTransform = new Function<Fact, View.Entity>() {
    @Override
    public View.Entity apply(Fact fact) {
      return new View.Entity(fact.getName(), View.Entity.ProcessPipeline.valueOf(fact.getTable().getProcessPipeline()), fact.getType(),fact.getDescription(),fact.getTable().getName(),fact.getTable().getProcessId());
    }
  };

  private final Function<Fact, View.Entity> factTransformIncludeNamespace = new Function<Fact, View.Entity>() {
    @Nullable
    @Override
    public View.Entity apply(Fact fact) {
      return new View.Entity(fact.getName(), View.Entity.ProcessPipeline.valueOf(fact.getTable().getProcessPipeline()), fact.getType(),fact.getDescription(),fact.getTable().getName(),fact.getTable().getProcessId(), new View.Namespace(fact.getNamespace()));
    }
  };

  private final Function<Table,View.Entity> tableTransform = new Function<Table, View.Entity>() {
    @Override
    public View.Entity apply(Table table) {
      return new View.Entity(table.getName(), View.Entity.ProcessPipeline.valueOf(table.getProcessPipeline()),table.getType(),table.getDescription(),table.getName(),table.getProcessId());
    }
  };

  private final Function<Table, View.Entity> tableTransformIncludingNamespace = new Function<Table, View.Entity>() {
    @Override
    public View.Entity apply(Table table) {
      return new View.Entity(table.getName(),
          View.Entity.ProcessPipeline.valueOf(table.getProcessPipeline()),
          table.getType(),
          table.getDescription(),
          table.getName(),
          table.getProcessId(),
          new View.Namespace(table.getNamespace()));
    }
  };

  public MetaAccessor(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
    this.transactionLender = new TransactionLender(sessionFactory);
  }


  /*----------------------------------------------------------------*/
  /*-----------company, org, namespace and tag API's-------------------------------*/

  //get all orgs
  public Set<String> getOrgs(){
    final OrgDAO dao = new OrgDAO(sessionFactory);
    final AtomicReference<Set<String>> orgs = new AtomicReference<Set<String>>(Sets.<String>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Org> orgSet = dao.getOrgs();
        Set<String> orgNames = Sets.newHashSet(Iterables.transform(orgSet, new Function<Org, String>() {
          @Override
          public String apply(Org input) {
            return input.getName();
          }
        }));
        orgs.set(orgNames);
      }
    });
    return orgs.get();
  }

  public Set<View.Namespace> getNamespaces(){
    final NamespaceDAO dao = new NamespaceDAO(sessionFactory);
    final AtomicReference<Set<View.Namespace>> namespaces = new AtomicReference<Set<View.Namespace>>(Sets.<View.Namespace>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Namespace> namespaceSet = dao.getNamespaces();
        Set<View.Namespace> namespacesView = Sets.newHashSet(Iterables.transform(namespaceSet, new Function<Namespace, View.Namespace>() {
          @Override
          public View.Namespace apply(Namespace input) {
            return new View.Namespace(input.getName(),input.getOrg().getName());
          }
        }));
        namespaces.set(namespacesView);
      }
    });
    return namespaces.get();
  }

  public View.Namespace getNamespaceByOrgAndName(final String org, final String name){
    final NamespaceDAO dao = new NamespaceDAO(sessionFactory);
    final AtomicReference<View.Namespace> namespace = new AtomicReference<View.Namespace>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Namespace internalNamespace = dao.getNamespaceByOrgAndName(org, name);
        namespace.set(new View.Namespace(org, name));
      }
    });
    return namespace.get();
  }

  public Set<View.Namespace> getNamespacesByOrg(final String org){
    final OrgDAO dao = new OrgDAO(sessionFactory);
    final AtomicReference<Set<View.Namespace>> namespaces = new AtomicReference<Set<View.Namespace>>(Sets.<View.Namespace>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Org internalOrg = dao.getOrgByName(org);
        Set<Namespace> namespaceSet = internalOrg.getNamespaces();
        Set<View.Namespace> namespacesView = Sets.newHashSet(Iterables.transform(namespaceSet,new Function<Namespace, View.Namespace>() {
          @Override
          public View.Namespace apply( Namespace input) {
            return new View.Namespace(input.getName(),input.getOrg().getName());
          }
        }));
        namespaces.set(namespacesView);
      }
    });
    return namespaces.get();
  }

  //get all tag related info
  public Map<String, Map<String, List<String>>> getAllData() {
    final TagDAO dao = new TagDAO(sessionFactory);
    final AtomicReference<Map<String, Map<String, List<String>>>> map = new AtomicReference<Map<String, Map<String, List<String>>>>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Map<String, Map<String, List<String>>> allInfoMap = dao.getAllData();
        map.set(allInfoMap);
      }
    });
    return map.get();
  }

  //get associated tags with data-source
  public List<String> getAssociatedTags(final String dataSourceName) {
    final TagDAO dao = new TagDAO(sessionFactory);
    final AtomicReference<List<String>> tags = new AtomicReference<List<String>>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        List<String> associatedTags = dao.getAssociatedTags(dataSourceName);
        tags.set(associatedTags);
      }
    });
    return tags.get();
  }

  //remove tag association with dataSource
  public void removeAssociation(final String dataSourceName, final TagAssociation.SchemaType type) {
    final TagDAO dao = new TagDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        dao.removeAssociation(dataSourceName, type);
      }
    });
  }

  //alter tag association with dataSource
  public void alterAssociation(final String dataSourceName, final String user, final TagAssociation.SchemaType type, final List<String> tagNames) {
    final TagDAO dao = new TagDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        dao.alterAssociation(dataSourceName, user, type, tagNames);
      }
    });
  }

  /*----------------------------------------------------------------*/
  /*-----------------source type, source and table APIs-------------*/
  // no additional APIs for columns as detailed view of table would contain column details as well

  // get all source types (will be needed for creating new sources)
  public Set<SourceType> getSourceTypes(){
    return (new SourceTypeDAO()).getSourceTypes();
  }

  public com.flipkart.fdp.superbi.cosmos.meta.model.external.Source getSourceByName(final String sourceName) {
    return getSourceByName(sourceName, Source.FederationType.DEFAULT);
  }

  public com.flipkart.fdp.superbi.cosmos.meta.model.external.Source getSourceByName(final String sourceName, final
  Source.FederationType federationType) {
    final SourceDAO dao = new SourceDAO(sessionFactory);
    final AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Source> sourceAtomicReference = new AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Source>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Source source = dao.getSourceByName(sourceName, federationType);
        sourceAtomicReference.set(new com.flipkart.fdp.superbi.cosmos.meta.model.external.Source(
            source.getName(),
            source.getSourceType().name(),
            source.getFederationType(),
            source.getAttributes(),
            source.getId()));
      }
    });
    return sourceAtomicReference.get();
  }

  public com.flipkart.fdp.superbi.cosmos.meta.model.external.Source getSourceById(final int id) {
    final SourceDAO dao = new SourceDAO(sessionFactory);
    final AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Source> sourceAtomicReference = new AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Source>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Source source = dao.getSourceById(id);
        sourceAtomicReference.set(new com.flipkart.fdp.superbi.cosmos.meta.model.external.Source(
            source.getName(),
            source.getSourceType().name(),
            source.getFederationType(),
            source.getAttributes(),
            source.getId()
        ));
      }
    });
    return sourceAtomicReference.get();
  }

  public Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Source> getAllFederatedSourcesByName(final String sourceName) {
    final SourceDAO dao = new SourceDAO(sessionFactory);
    final AtomicReference<Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Source>> sourceAtomicReference = new AtomicReference<Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Source>>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Source> sources = dao.getAllFederatedSourcesByName(sourceName);
        Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Source> sourceSet = new HashSet<>();
        sources.forEach(source -> {
          sourceSet.add(
              new com.flipkart.fdp.superbi.cosmos.meta.model.external.Source(
                  source.getName(),
                  source.getSourceType().name(),
                  source.getFederationType(),
                  source.getAttributes(),
                  source.getId())
          );
        });
        sourceAtomicReference.set(sourceSet);
      }
    });
    return sourceAtomicReference.get();
  }

  // get all sources (querying a particular source is not required as creating a table from source requires only source name which can be selected from this API)
  public Set<View.Source> getSources(){
    final SourceDAO dao = new SourceDAO(sessionFactory);
    final AtomicReference<Set<View.Source>> sources = new AtomicReference<Set<View.Source>>(Sets.<View.Source>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Source> sourceSet = dao.getSources();
        Set<View.Source> sourceViews = Sets.newHashSet(Iterables.transform(sourceSet, new Function<Source, View.Source>() {
          @Override
          public View.Source apply(Source source) {
            return new View.Source(source.getName(), source.getSourceType().name(), source.getFederationType(), source.getId());
          }
        }));
        sources.set(sourceViews);
      }
    });
    return sources.get();
  }

  // get all tables (optional restriction to a particular process pipeline)
  public Set<View.Entity> getTables(){
    return getTables(Optional.<View.Entity.ProcessPipeline>absent());
  }

  public Set<View.Entity> getTables(final Optional<View.Entity.ProcessPipeline> processPipeline) {
    final TableDAO dao = new TableDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> tableViews = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Table> tables = dao.getTables(processPipeline);
        tableViews.set(Sets.newHashSet(Iterables.transform(tables,tableTransform)));
      }
    });
    return tableViews.get();
  }

  public Set<View.Entity> getTablesWithNamespaceOrg(){
    return getTablesWithNamespaceOrg(Optional.<View.Entity.ProcessPipeline>absent());
  }

  public Set<View.Entity> getTablesWithNamespaceOrg(final Optional<View.Entity.ProcessPipeline> processPipeline) {
    final TableDAO dao = new TableDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> tableViews = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Table> tables = dao.getTables(processPipeline);
        tableViews.set(Sets.newHashSet(Iterables.transform(tables,tableTransformIncludingNamespace)));
      }
    });
    return tableViews.get();
  }

  //get detailed table info by name
  public com.flipkart.fdp.superbi.cosmos.meta.model.external.Table getTableByName(final String tableName){
    final TableDAO dao = new TableDAO(sessionFactory);
    final AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Table> table = new AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Table>
        (new com.flipkart.fdp.superbi.cosmos.meta.model.external.Table());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Table internalTable = dao.getTableByName(tableName);
        table.set(new com.flipkart.fdp.superbi.cosmos.meta.model.external.Table(internalTable));
      }
    });
    return table.get();
  }


  /*-------------------------------------------------------------------------*/
  /*-------------------------dimension APIs----------------------------------*/

  //get all dimensions (optional restriction to a particular process pipeline)
  public Set<View.Entity> getDimensions(){
    return getDimensions(Optional.<View.Entity.ProcessPipeline>absent());
  }

  public Set<View.Entity> getDimensions(Optional<View.Entity.ProcessPipeline> processPipeline){
    final View.Entity.ProcessPipeline pipeline = processPipeline.isPresent()? processPipeline.get() : View.Entity.ProcessPipeline.all;
    final DimensionDAO dimensionDAO = new DimensionDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> dimensions = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Dimension> dims = dimensionDAO.getDimensions();
        Set<Dimension> filteredDims = dims;
        if(!pipeline.name().equals("all")){
          final String pipelineName = pipeline.name();
          filteredDims = Sets.newHashSet(Iterables.filter(dims, new Predicate<Dimension>() {
            @Override
            public boolean apply(Dimension dimension) {
              boolean result = false;
              if(dimension.getTable().getProcessPipeline().equals(pipelineName))
                result = true;
              return  result;
            }
          }));
        }
        Set<View.Entity> dimEntities = Sets.newHashSet(Iterables.transform(filteredDims,dimTransform));
        dimensions.set(dimEntities);
      }
    });
    return dimensions.get();
  }

  public Set<View.Entity> getDimensionsWithNamespaceOrg(){
    return getDimensionsWithNamespaceOrg(Optional.<View.Entity.ProcessPipeline>absent());
  }

  public Set<View.Entity> getDimensionsWithNamespaceOrg(Optional<View.Entity.ProcessPipeline> processPipeline){
    final View.Entity.ProcessPipeline pipeline = processPipeline.isPresent()? processPipeline.get() : View.Entity.ProcessPipeline.all;
    final DimensionDAO dimensionDAO = new DimensionDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> dimensions = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Dimension> dims = dimensionDAO.getDimensions();
        Set<Dimension> filteredDims = dims;
        if(!pipeline.name().equals("all")){
          final String pipelineName = pipeline.name();
          filteredDims = Sets.newHashSet(Iterables.filter(dims, new Predicate<Dimension>() {
            @Override
            public boolean apply(Dimension dimension) {
              boolean result = false;
              if(dimension.getTable().getProcessPipeline().equals(pipelineName))
                result = true;
              return  result;
            }
          }));
        }
        Set<View.Entity> dimEntities = Sets.newHashSet(Iterables.transform(filteredDims,dimTransformIncludeNamespace));
        dimensions.set(dimEntities);
      }
    });
    return dimensions.get();
  }

  // get detailed info for a dimension by name
  public com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension getDimensionByName(final String dimensionName){
    final DimensionDAO dao = new DimensionDAO(sessionFactory);
    final AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension> dimensionView = new AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension>
        (new com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Dimension internalDimension = dao.getDimensionByName(dimensionName);
        dimensionView.set(new com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension(internalDimension));
      }
    });
    return dimensionView.get();
  }

  // get all dimensions for a fact (by fact name)
  public Set<View.Entity> getDimensionsByFact(final String factName){
    final FactDimensionMappingDAO dao = new FactDimensionMappingDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> dimensions = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Dimension> dims= dao.getDimensionsByFact(factName);
        Set<View.Entity> dimEntities = Sets.newHashSet(Iterables.transform(dims,dimTransform));
        dimensions.set(dimEntities);
      }
    });
    Set<View.Entity> dimEntities = dimensions.get();
    Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension> degenerateDimensions = getDegenerateDimensionsByFactName(factName);
    for(com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension degenerateDimension: degenerateDimensions)
      dimEntities.add(new View.DimensionEntity(degenerateDimension.getName(),null, DataSource.Type.dimension,degenerateDimension.getDescription(),true));
    return dimEntities;
  }

  // get information of hierarchy by dimension and hierarchy name
  public Optional<com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Hierarchy> getHierarchy(final String dimension,final  String hierarchyName){
    final DimensionDAO dao = new DimensionDAO(sessionFactory);
    final AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Hierarchy> hierarchy = new AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Hierarchy>(null);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Optional<Hierarchy> internalHierarchy = dao.getHierarchy(dimension,hierarchyName);
        if(internalHierarchy.isPresent()){
          com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Hierarchy h1 = new com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Hierarchy(internalHierarchy.get());
          hierarchy.set(h1);
        }
      }
    });
    if(hierarchy.get() == null)
      return Optional.absent();
    else
      return Optional.of(hierarchy.get());
  }

  public Optional<com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Level> getLevel(final String dimensionName, final String hierarchyName, final String levelName){
    final DimensionDAO dao = new DimensionDAO(sessionFactory);
    final AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Level> level = new AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Level>(null);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Optional<Level> internalLevel = dao.getLevel(dimensionName,hierarchyName,levelName);
        if(internalLevel.isPresent()){
          com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Level l1 = new com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Level(internalLevel.get());
          level.set(l1);
        }
      }
    });
    if(level.get() == null)
      return Optional.absent();
    else return Optional.of(level.get());
  }

  /*-------------------------------------------------------------------------*/
  /*--------------------------fact APIs--------------------------------------*/

  // get all facts (optional restriction to a particular process pipeline)
  public Set<View.Entity> getFacts(){
    return getFacts(Optional.<View.Entity.ProcessPipeline>absent());
  }

  public Set<View.Entity> getFacts(final Optional<View.Entity.ProcessPipeline> processPipeline){
    final View.Entity.ProcessPipeline pipeline = processPipeline.isPresent()? processPipeline.get() : View.Entity.ProcessPipeline.all;
    final FactDAO dao = new FactDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> facts = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Fact> internalFacts = dao.getFacts();
        Set<Fact> filteredFacts = internalFacts;
        if(!pipeline.name().equals("all")){
          final String pipelineName = pipeline.name();
          filteredFacts = Sets.newHashSet(Iterables.filter(internalFacts, new Predicate<Fact>() {
            @Override
            public boolean apply(Fact fact) {
              boolean result = false;
              if(fact.getTable().getProcessPipeline().equals(pipelineName))
                result = true;
              return  result;
            }
          }));
        }
        Set<View.Entity> factEntities = Sets.newHashSet(Iterables.transform(filteredFacts,factTransform));
        facts.set(factEntities);
      }
    });
    return facts.get();
  }

  public Set<View.Entity> getFactsWithNamespaceOrg() {
    return getFactsWithNamespaceOrg(Optional.<View.Entity.ProcessPipeline>absent());
  }

  public Set<View.Entity> getFactsWithNamespaceOrg(final Optional<View.Entity.ProcessPipeline> processPipeline) {
    final View.Entity.ProcessPipeline pipeline = processPipeline.isPresent()? processPipeline.get() : View.Entity.ProcessPipeline.all;
    final FactDAO dao = new FactDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> facts = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Fact> internalFacts = dao.getFacts();
        Set<Fact> filteredFacts = internalFacts;
        if(!pipeline.name().equals("all")){
          final String pipelineName = pipeline.name();
          filteredFacts = Sets.newHashSet(Iterables.filter(internalFacts, new Predicate<Fact>() {
            @Override
            public boolean apply(Fact fact) {
              boolean result = false;
              if(fact.getTable().getProcessPipeline().equals(pipelineName))
                result = true;
              return  result;
            }
          }));
        }
        Set<View.Entity> factEntities = Sets.newHashSet(Iterables.transform(filteredFacts,
            factTransformIncludeNamespace));
        facts.set(factEntities);
      }
    });
    return facts.get();
  }

  public Set<View.Entity> getFactsByStorageType(final SourceType sourceType) {
    final FactDAO factDAO = new FactDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> facts = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());

    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Fact> internalFacts = factDAO.getFacts();
        Set<Fact> filteredFacts;

        filteredFacts = Sets.newHashSet(Iterables.filter(internalFacts, new Predicate<Fact>() {
          @Override
          public boolean apply(Fact fact) {
            return fact.getTable().getSource().getSourceType() == sourceType ? true : false;
          }
        }));

        Set<View.Entity> factEntities = Sets.newHashSet(Iterables.transform(filteredFacts,
            factTransformIncludeNamespace));
        facts.set(factEntities);
      }
    });
    return facts.get();
  }

  // get detailed info for a fact by name (with dimension mappings)
  public com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact getFactByName(final String factName) {
    final FactDAO dao = new FactDAO(sessionFactory);
    final FactDimensionMappingDAO factDimensionMappingDAO = new FactDimensionMappingDAO(sessionFactory);
    final AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact> factView = new AtomicReference<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact>
        (new com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Fact internalFact = dao.getFactByName(factName);
        Set<FactDimensionMapping> factDimensionMappings = factDimensionMappingDAO.getDimensionMappingByFact(internalFact);
        Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping> dimensionMappings = Sets.newHashSet(Iterables.transform(factDimensionMappings, new Function<FactDimensionMapping, com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping>() {
          @Override
          public com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping apply(com.flipkart.fdp.superbi.cosmos.meta.model.data.FactDimensionMapping factDimensionMapping) {
            return new com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping(factDimensionMapping);
          }
        }));
        com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact fact = new com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact(internalFact);
        factView.set(fact);
      }
    });
    return factView.get();
  }

  public Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping> getFactDimensionMappings(final Fact internalFact) {
    final FactDimensionMappingDAO factDimensionMappingDAO = new FactDimensionMappingDAO(sessionFactory);
    final AtomicReference<Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping>> mappingRef
        = new AtomicReference<Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping>>();
    transactionLender.execute(
        new WorkUnit() {
          @Override
          public void actualWork() {
            Set<FactDimensionMapping> factDimensionMappings = factDimensionMappingDAO.getDimensionMappingByFact(internalFact);
            Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping> dimensionMappings = Sets.newHashSet(Iterables.transform(factDimensionMappings, new Function<FactDimensionMapping, com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping>() {
              @Override
              public com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping apply(com.flipkart.fdp.superbi.cosmos.meta.model.data.FactDimensionMapping factDimensionMapping) {
                return new com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping(factDimensionMapping);
              }
            }));
            mappingRef.set(dimensionMappings);
          }
        }
    );
    return mappingRef.get();
  }

  @Deprecated
    /*
     Added for testing, don't use anywhere.
     */
  public Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping> getFactDimensionMappings(final String factName) {

    final DataSourceDAO dao = new DataSourceDAO(sessionFactory);
    final AtomicReference<Fact> entityRef = new AtomicReference<Fact>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        entityRef.set((Fact) dao.getBy(DataSource.Type.fact, factName));
      }
    });
    return getFactDimensionMappings(entityRef.get());
  }

  // get all facts for a dimension (by name)
  public Set<View.Entity> getFactsByDimensionName(final String dimensionName){
    final FactDimensionMappingDAO dao = new FactDimensionMappingDAO(sessionFactory);
    final AtomicReference<Set<View.Entity>> facts = new AtomicReference<Set<View.Entity>>(Sets.<View.Entity>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Set<Fact> internalFacts= dao.getFactsByDimension(dimensionName);
        Set<View.Entity> factEntities = Sets.newHashSet(Iterables.transform(internalFacts,factTransform));
        facts.set(factEntities);
      }
    });
    return facts.get();
  }

  // get all degenerate dimensions for a fact
  public Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension> getDegenerateDimensionsByFactName(final String factName){
    final FactDAO dao = new FactDAO(sessionFactory);
    final AtomicReference<Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension>>  degenerateDims = new AtomicReference<Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension>>
        (Sets.<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension>newHashSet());
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        Fact fact = dao.getFactByName(factName);
        Set<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension> degenerateDimensions = Sets.newHashSet(
            Iterables.transform(fact.getDegenerateDimensions(),new Function<DegenerateDimension, com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension>() {
              @Override
              public com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension apply(com.flipkart.fdp.superbi.cosmos.meta.model.data.DegenerateDimension degenerateDimension) {
                return new com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension(degenerateDimension);
              }
            }));
        degenerateDims.set(degenerateDimensions);
      }
    });
    return degenerateDims.get();
  }


  /*-------general entity view api-----------------------*/

  public View.Entity getEntityBy(final String dataSourceName) {
    // TODO remove assumption table name format is [a-zA-Z1-9_].[a-zA-Z1-9_] and fact name format is [a-zA-Z1-9_]
    String[] fromSource =  dataSourceName.split("\\.");
    if(fromSource.length == 1) {
      // fact is provided
      return getEntity(dataSourceName, DataSource.Type.fact);
    } else if(fromSource.length == 2) {
      return getEntity(dataSourceName, DataSource.Type.raw);
    } else {
      throw new RuntimeException("from table should either be a fact or raw table");
    }
  }

  public View.Entity getEntity(final String name,final DataSource.Type type){
    final DataSourceDAO dao = new DataSourceDAO(sessionFactory);
    final AtomicReference<View.Entity> entityRef = new AtomicReference<View.Entity>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        entityRef.set(
            dao.getBy(type, name).asWebEntityDetailed()
        );
      }
    });
    return entityRef.get();
  }

  public Map<String,String> getTablePropertiesByTableName(String name){
    final TablePropertiesDao dao = new TablePropertiesDao(sessionFactory);
    final AtomicReference<Map<String,String>> map = new AtomicReference<Map<String,String>>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        TableProperties result = dao.getPropertiesByName(name);
        if(result != null)
          map.set(MapUtil.stringToMap(result.getProperties()));
      }
    });
    return map.get();
  }

  public Map<String,String> getTablePropertiesFromCache(String name){

    try {
      return tablePropertiesLoadingCache.get(name);
    }catch (ExecutionException e){
      return getTablePropertiesByTableName(name);
    }
  }
  public Collection<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact> getFactsAsFacts(){
    List<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact> facts = Lists.newArrayList();
    for(View.Entity entity : getFacts()) {
      facts.add(getFactByName(entity.getName()));
    }
    return facts;
  }


  /*---------Get jasper domain xml for a namespace ----------*/
  public JasperDomain getJasperDomain(final String org,final String namespace) {
    final AtomicReference<JasperDomain> jasperDomainAtomicReference = new AtomicReference<JasperDomain>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        final NamespaceDomainMapping mapping = new NamespaceDomainMappingDao(sessionFactory).getFor(org, namespace);
        jasperDomainAtomicReference.set(
            new JasperDomain(mapping.getDomainSchemaXML(), mapping.getDomainSecurityXML())
        );
      }
    });

    return jasperDomainAtomicReference.get();
  }

  public EMondrianSchema getMondrianSchema(final String org, final String namespace, final String sourceName, final int version) {
    final AtomicReference<EMondrianSchema> mondrianSchemaRef = new AtomicReference<EMondrianSchema>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        final Optional<MondrianSchema> mappingOptional = new MondrianSchemaDao(sessionFactory)
            .getLastButNthVersion(org, namespace, sourceName, version);
        if(!mappingOptional.isPresent()) throw new RuntimeException("Not present");
        final MondrianSchema mapping = mappingOptional.get();
        mondrianSchemaRef.set(
            MondrianSchema.F.toWebEntity.apply(mapping)
        );
      }
    });

    return mondrianSchemaRef.get();
  }

  public EMondrianSchema getMondrianSchema(final String org, final String namespace, final String sourceName) {
    final AtomicReference<EMondrianSchema> mondrianSchemaRef = new AtomicReference<EMondrianSchema>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        final Optional<MondrianSchema> mappingOptional = new MondrianSchemaDao(sessionFactory)
            .getLatest(org, namespace, sourceName);
        if(!mappingOptional.isPresent()) throw new RuntimeException("Not present");
        final MondrianSchema mapping = mappingOptional.get();
        mondrianSchemaRef.set(
            MondrianSchema.F.toWebEntity.apply(mapping)
        );
      }
    });

    return mondrianSchemaRef.get();
  }

  public List<EMondrianSchema> getMondrianSchema(final String org, final String namespace) {
    final AtomicReference<List<EMondrianSchema>> mondrianSchemaRef = new AtomicReference<List<EMondrianSchema>>();
    final NamespaceDAO nsDao = new NamespaceDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        final List<MondrianSchema> schemaList = new MondrianSchemaDao(sessionFactory)
            .getBy(nsDao.getNamespaceByOrgAndName(org, namespace));
        mondrianSchemaRef.set(
            Lists.newArrayList(Lists.transform(schemaList, MondrianSchema.F.toWebEntity))
        );
      }
    });

    return mondrianSchemaRef.get();
  }

  public List<EMondrianSchema> getMondrianSchema(final String org) {
    final AtomicReference<List<EMondrianSchema>> mondrianSchemaRef = new AtomicReference<List<EMondrianSchema>>();
    final OrgDAO orgDAO = new OrgDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        final List<MondrianSchema> schemaList = new MondrianSchemaDao(sessionFactory)
            .getBy(orgDAO.getOrgByName(org));
        mondrianSchemaRef.set(
            Lists.newArrayList(Lists.transform(schemaList, MondrianSchema.F.toWebEntity))
        );
      }
    });

    return mondrianSchemaRef.get();
  }

  public List<EMondrianSchema> getMondrianSchema() {
    final AtomicReference<List<EMondrianSchema>> mondrianSchemaRef = new AtomicReference<List<EMondrianSchema>>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        final List<MondrianSchema> schemaList = new MondrianSchemaDao(sessionFactory)
            .getAll();
        mondrianSchemaRef.set(
            Lists.newArrayList(Lists.transform(schemaList, MondrianSchema.F.toWebEntity))
        );
      }
    });

    return mondrianSchemaRef.get();
  }


  public Set<WebBoltLookup> getBoltLookupByTableName(final String factTableName){
    final AtomicReference<Set<WebBoltLookup>> webBoltLookupAtomicReference = new AtomicReference<Set<WebBoltLookup>>();
    final BoltLookupDAO boltLookupDAO = new BoltLookupDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        List<BoltLookup> boltLookups = boltLookupDAO.getAllBoltLookupByTableName(factTableName);
        Set<BoltLookup> boltLookupSet = new HashSet<BoltLookup>(boltLookups);
        webBoltLookupAtomicReference.set(Sets.newHashSet(Iterables.transform(boltLookupSet, boltLookupTransform)));
      }
    });
    return webBoltLookupAtomicReference.get();
  }

  private final Function<BoltLookup, WebBoltLookup> boltLookupTransform = new Function<BoltLookup, WebBoltLookup>() {
    @Override
    public WebBoltLookup apply(BoltLookup boltLookup) {
      return new WebBoltLookup(boltLookup);
    }
  };


  public Set<WebBoltSchemaMap> getBoltSchemaMapByTableName(final String factTableName){
    final AtomicReference<Set<WebBoltSchemaMap>> webBoltSchemaMapAtomicReference = new AtomicReference<Set<WebBoltSchemaMap>>();
    final BoltSchemaMapDAO boltSchemaMapDAO = new BoltSchemaMapDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        List<BoltSchemaMap> boltSchemaMaps = boltSchemaMapDAO.getBoltSchemaMapsByTable(factTableName);
        Set<BoltSchemaMap> boltSchemaMapSet = new HashSet<BoltSchemaMap>(boltSchemaMaps);
        webBoltSchemaMapAtomicReference.set(Sets.newHashSet(Iterables.transform(boltSchemaMapSet, boltSchemaMapTransform)));
      }
    });
    return webBoltSchemaMapAtomicReference.get();
  }

  private final Function<BoltSchemaMap, WebBoltSchemaMap> boltSchemaMapTransform = new Function<BoltSchemaMap, WebBoltSchemaMap>() {
    @Override
    public WebBoltSchemaMap apply(BoltSchemaMap boltSchemaMap) {
      return new WebBoltSchemaMap(boltSchemaMap);
    }
  };

  public WebBoltLookup getUniqueBoltLookup(final String factTableName, final String lookupTableName, final String lookupName){
    final AtomicReference<WebBoltLookup> webBoltLookupAtomicReference = new AtomicReference<WebBoltLookup>();
    final BoltLookupDAO boltLookupDAO = new BoltLookupDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        BoltLookup boltLookup = boltLookupDAO.getBoltLookupByNames(factTableName, lookupTableName, lookupName);
        WebBoltLookup webBoltLookup = new WebBoltLookup(boltLookup);
        webBoltLookupAtomicReference.set(webBoltLookup);
      }
    });
    return webBoltLookupAtomicReference.get();
  }

  public WebBoltSchemaMap getUniqueBoltSchemaMap(final String factTableName, final String columnName){
    final AtomicReference<WebBoltSchemaMap> webBoltSchemaMapAtomicReference = new AtomicReference<WebBoltSchemaMap>();
    final BoltSchemaMapDAO boltSchemaMapDAO = new BoltSchemaMapDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        BoltSchemaMap boltSchemaMap = boltSchemaMapDAO.getBoltSchemaMap(factTableName, columnName);
        WebBoltSchemaMap webBoltSchemaMap = new WebBoltSchemaMap(boltSchemaMap);
        webBoltSchemaMapAtomicReference.set(webBoltSchemaMap);
      }
    });
    return webBoltSchemaMapAtomicReference.get();
  }

  public Set<WebBoltPrimaryEntityMap> getBoltPrimaryEntityMapByTableName(final String factTableName){
    final AtomicReference<Set<WebBoltPrimaryEntityMap>> webBoltPrimaryEntityMapAtomicReference = new AtomicReference<Set<WebBoltPrimaryEntityMap>>();
    final BoltPrimaryEntityMapDAO boltPrimaryEntityMapDAO = new BoltPrimaryEntityMapDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        List<BoltPrimaryEntityMap> boltPrimaryEntityMaps = boltPrimaryEntityMapDAO.getBoltPrimaryEntityMapByTable(factTableName);
        Set<BoltPrimaryEntityMap> boltPrimaryEntityMapSet = new HashSet<BoltPrimaryEntityMap>(boltPrimaryEntityMaps);
        webBoltPrimaryEntityMapAtomicReference.set(Sets.newHashSet(Iterables.transform(boltPrimaryEntityMapSet,boltPrimaryEntityMapTransform)));
      }
    });
    return webBoltPrimaryEntityMapAtomicReference.get();
  }

  private final Function<BoltPrimaryEntityMap, WebBoltPrimaryEntityMap> boltPrimaryEntityMapTransform = new Function<BoltPrimaryEntityMap, WebBoltPrimaryEntityMap>() {
    @Override
    public WebBoltPrimaryEntityMap apply(BoltPrimaryEntityMap boltPrimaryEntityMap) {
      return new WebBoltPrimaryEntityMap(boltPrimaryEntityMap);
    }
  };

  public WebBoltPrimaryEntityMap getUniqueBoltPrimaryEntityMap(final String factTableName){
    final AtomicReference<WebBoltPrimaryEntityMap> webBoltPrimaryEntityMapAtomicReference = new AtomicReference<WebBoltPrimaryEntityMap>();
    final BoltPrimaryEntityMapDAO boltPrimaryEntityMapDAO = new BoltPrimaryEntityMapDAO(sessionFactory);
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        BoltPrimaryEntityMap boltPrimaryEntityMap = boltPrimaryEntityMapDAO.getBoltPrimaryEntityMap(factTableName);
        WebBoltPrimaryEntityMap webBoltPrimaryEntityMap = new WebBoltPrimaryEntityMap(boltPrimaryEntityMap);
        webBoltPrimaryEntityMapAtomicReference.set(webBoltPrimaryEntityMap);
      }
    });
    return webBoltPrimaryEntityMapAtomicReference.get();
  }

  public List<com.flipkart.fdp.superbi.cosmos.meta.model.external.Table.Column> getEntityColumns(String dataSourceName) {
    switch (getEntityType(dataSourceName)) {
      case fact:
        return MetaAccessor.get().getTableByName(
            MetaAccessor.get().getFactByName(dataSourceName).getTableName()
        ).getColumns();
      case raw:
        return MetaAccessor.get().getTableByName(dataSourceName).getColumns();
    }
    throw new RuntimeException("Invalid datasourcename");
  }

  public DataSource.Type getEntityType(String dataSourceName) {

    final DataSourceDAO dataSourceDAO = new DataSourceDAO(sessionFactory);
    final AtomicReference<DataSource.Type> dataSourceTypeReference = new AtomicReference<>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        DataSource dataSource = dataSourceDAO.getDataSourceByName(dataSourceName);
        dataSourceTypeReference.set(dataSource.getType());
      }
    });

    return dataSourceTypeReference.get();
  }

  public List<View.Entity> getDataSources(final Set<String> resourceURI,
      final Map<String, String[]> filters,
      final int offset,
      final int limit,
      final boolean detailed) {
    final DataSourceDAO dataSourceDAO = new DataSourceDAO(sessionFactory);
    final AtomicReference<List<View.Entity>> sources = new AtomicReference<List<View.Entity>>();
    transactionLender.execute(
        new WorkUnit() {
          @Override
          public void actualWork() {
            final List<View.Entity> entityList = Lists.newArrayList();
            for(DataSource ds : dataSourceDAO.getMatching(resourceURI, filters, offset, limit)) {
              entityList.add(
                  detailed?
                      ds.asWebEntityDetailed():
                      ds.asWebEntityMinimal()
              );
            }
            sources.set(entityList);
          }
        }
    );
    return sources.get();
  }


  public List<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension> getDegeneratesBelongingToTheSameHierarchyAs(final String factName, final String degDimName) {

    final DegenerateDimensionDAO dao = new DegenerateDimensionDAO(sessionFactory);
    final AtomicReference<List<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension>> ref = new AtomicReference<List<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension>>();
    transactionLender.execute(
        new WorkUnit() {
          @Override
          public void actualWork() {
            List<com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension> degDims = Lists.newArrayList();
            for(DegenerateDimension dim :dao.getDegeneratesBelongingToTheSameHierarchyAs(factName, degDimName)) {
              degDims.add(new com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension(dim));
            }
            ref.set(degDims);
          }
        }
    );
    return ref.get();
  }

  public Date getLastTableUpdateTime (String tableName) {
    final TableDAO tableDAO = new TableDAO(sessionFactory);
    final AtomicReference<Date> lastUpdateTimeAtomicReference = new AtomicReference<>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        lastUpdateTimeAtomicReference.set(tableDAO.getLastUpdateTime(tableName));
      }
    });
    return lastUpdateTimeAtomicReference.get();
  }

  public ExecutorQueryInfoLog getExecutorQueryInfo(long id) {
    final ExecutorQueryInfoDAO executorQueryInfoDAO = new ExecutorQueryInfoDAO(sessionFactory);
    final AtomicReference<ExecutorQueryInfoLog> atomicReference = new AtomicReference<>();
    transactionLender.execute(new WorkUnit() {
      @Override
      public void actualWork() {
        atomicReference.set(executorQueryInfoDAO.getById(id));
      }
    });
    return atomicReference.get();
  }

  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }

}
