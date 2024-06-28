package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Source;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.SessionFactory;

/**
 * User: aniruddha.gangopadhyay
 * Date: 27/02/14
 * Time: 10:44 PM
 */
public class SourceDAO extends AbstractDAO<Source> {
  /**
   * Creates a new DAO with a given session provider.
   *
   * @param sessionFactory a session provider
   */
  public SourceDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  /**
   * @param source
   * @throws RuntimeException if source already exists or source type is invalid
   */
  public void save(com.flipkart.fdp.superbi.cosmos.meta.model.external.Source source) {
    Source existingSource = null;
    try {
      existingSource = getSourceByName(source.getName(), source.getFederationType());
    } catch (RuntimeException e) {
    }
    if (existingSource != null) {
      throw new RuntimeException("Source :" + source.getName() + " already exists!!");
    }
//        Source internalSource = new com.flipkart.fdp.superbi.cosmos.meta.model.data.Source();
//        SourceType sourceType = getSourceType(source.getSourceType());
//        internalSource.setName(source.getName());
//        internalSource.setAttributes(source.getAttributes().toString());
//        internalSource.setSourceType(sourceType);
    Source internalSource = getFromExternalView(source);
    internalSource.setVersion(1);
    currentSession().save(internalSource);
  }

  private SourceType getSourceType(String sourceTypeName) {
    return SourceType.valueOf(sourceTypeName);
  }

  public Source getSourceByName(String name) {
    return getSourceByName(name, Source.FederationType.DEFAULT);
  }

  public Source getSourceById(int id) {
    return get(id);
  }

  public Source getSourceByName(String name, Source.FederationType federationType) {
    Source source = uniqueResult(criteria().add(eq("name", name)).add(eq("deleted", false)).add(
        eq("federationType", federationType)));
    if (source == null) {
      throw new RuntimeException("no source by name : " + name);
    }
    return source;
  }

  public Set<Source> getSources() {
    return Sets.newHashSet(list(criteria().add(eq("deleted", false))));
  }

  public Set<Source> getAllFederatedSourcesByName(String sourceName) {
    return Sets.newHashSet(list(criteria().add(eq("deleted", false)).add(eq("name", sourceName))));
  }

  public void delete(String sourceName, Source.FederationType federationType) {
    Source source = getSourceByName(sourceName, federationType);
    source.setDeleted(true);
    currentSession().update(source);
  }

  public void delete(String sourceName) {
    Set<Source> sources = getAllFederatedSourcesByName(sourceName);
    if(sources != null) {
      sources.forEach(source -> {
        source.setDeleted(true);
        currentSession().update(source);
      });
    }
  }

  public void update(com.flipkart.fdp.superbi.cosmos.meta.model.external.Source source, Source.FederationType oldFederationType) {
    Source internalSource = null;
    try {
      internalSource = getSourceByName(source.getName(), oldFederationType);
    } catch (Exception e) {
    }
    if (internalSource == null) {
      save(source);
    } else {
      Source updatedSource = getFromExternalView(source);
      updatedSource.setVersion(internalSource.getVersion() + 1);
      delete(source.getName(), oldFederationType);
      currentSession().save(updatedSource);
    }
  }

  private Source getFromExternalView(
      com.flipkart.fdp.superbi.cosmos.meta.model.external.Source source) {
    Source internalSource = new com.flipkart.fdp.superbi.cosmos.meta.model.data.Source();
    SourceType sourceType = getSourceType(source.getSourceType());
    internalSource.setName(source.getName());
    internalSource.setAttributes(source.getAttributes().toString());
    internalSource.setSourceType(sourceType);
    internalSource.setFederationType(source.getFederationType());
    return internalSource;
  }

}
