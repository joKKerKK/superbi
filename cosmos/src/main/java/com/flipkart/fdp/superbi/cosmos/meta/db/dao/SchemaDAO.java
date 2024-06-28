package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.model.data.Attribute;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Schema;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;

/**
 * User: aartika
 * Date: 4/8/14
 */
public class SchemaDAO extends BaseDAO<Schema> {

    public SchemaDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Schema getEntityByName(String company, String org, String namespace, String name) {
        return schemaByName(company, org, namespace, name, Schema.SchemaType.entity);
    }

    public Schema getTypeByName(String company, String org, String namespace, String name) {
        return schemaByName(company, org, namespace, name, Schema.SchemaType.composite_type);
    }

    public Schema getEventByName(String company, String org, String namespace, String name) {
        return schemaByName(company, org, namespace, name, Schema.SchemaType.event);
    }

    public Schema schemaByName(String company, String org, String namespace, String name, Schema.SchemaType schemaType) {

        List<Schema> result = queryByName(company, org, namespace, name, schemaType)
                    .setBoolean("deleted", false)
                    .list();
        if (result.size() > 1)
            throw new IllegalStateException("There are more than 1 entities by name: " + name);

        Schema output = result.size() == 0 ? null : result.iterator().next();

        Schema tempSchema = output;
        if(tempSchema == null) return null;
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
        return  output;
    }

    public Schema schemaRegistered(String company, String org, String namespace, String name, Schema.SchemaType schemaType) {

        Query query = currentSession().createQuery("select ss from Schema ss join ss.company as com join ss.namespace as ns join ns.org as org where ss.schemaType =:schemaType" +
                " and com.name = :company and org.name=:org and ns.name=:namespace and ss.name=:name")
                .setString("schemaType", schemaType.toString())
                .setString("company", company)
                .setString("org", org)
                .setString("namespace", namespace)
                .setString("name", name);
        List<Schema> allSchemas = query
                .list();

        List<Schema> result = new ArrayList<Schema>();

        for ( Schema schema : allSchemas ){
            if(schema.getDartVersion() != null){
                result.add(schema);
                System.out.println("DART VERSION :" + schema.getDartVersion());
            }
        }
        Schema output =  result.size() == 0 ? null : result.get(result.size()-1);
        if(output==null) {
            return null;
        }
        Schema tempSchema = output;
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
        return  output;
    }

    public Schema schemaByNameAndVersion(String company, String org, String namespace, String name,
                                         Schema.SchemaType schemaType, int schemaVersion) {
        return uniqueResult(currentSession().createQuery("select ss from Schema ss join ss.company as com join ss.namespace as ns join ns.org as org where ss.schemaType =:schemaType" +
                " and com.name = :company and org.name=:org and ns.name=:namespace and ss.name=:name and ss.schemaVersion =:schemaVersion")
                .setString("schemaType", schemaType.toString())
                .setString("company", company)
                .setString("org", org)
                .setString("namespace", namespace)
                .setString("name", name)
                .setInteger("schemaVersion", schemaVersion));
    }

    public Query queryByName(String company, String org, String namespace, String name, Schema.SchemaType schemaType) {

        return currentSession().createQuery("select ss from Schema ss join ss.company as com join ss.namespace as ns join ns.org as org where ss.schemaType =:schemaType" +
                " and com.name = :company and org.name=:org and ns.name=:namespace and ss.name=:name and ss.deleted =:deleted")
                .setString("schemaType", schemaType.toString())
                .setString("company", company)
                .setString("org", org)
                .setString("namespace", namespace)
                .setString("name", name);
    }

    public List<Schema> getWithFilters(Map<String, Object> filters){
        Criteria criteria = criteria().createAlias("namespace", "namespace").createAlias("namespace.org", "org")
                .createAlias("company", "company");
        for(Map.Entry<String, Object> entry : filters.entrySet()){
            criteria = criteria.add(eq(entry.getKey(), entry.getValue()));
        }
        return criteria.add(eq("deleted", false))
                .add(eq("namespace.deleted", false))
                .add(eq("org.deleted", false))
                .list();

    }

    public List<Schema> getNonCompositeSchemasWithFilters(Map<String, Object> filters){
        Criteria criteria = criteria().createAlias("namespace", "namespace").createAlias("namespace.org", "org")
                .createAlias("company", "company");
        for(Map.Entry<String, Object> entry : filters.entrySet()){
            criteria = criteria.add(eq(entry.getKey(), entry.getValue()));
        }
        return criteria.add(eq("deleted", false))
                .add(eq("namespace.deleted", false))
                .add(eq("org.deleted", false))
                .add(Restrictions.ne("schemaType", Schema.SchemaType.composite_type))
                .list();

    }

    public List<Schema> getAllEntities() {
        return getAllSchemas(Schema.SchemaType.entity);
    }

    public List<Schema> getAllTypes() {
        return getAllSchemas(Schema.SchemaType.composite_type);
    }

    public List<Schema> getAllEvents() {
        return getAllSchemas(Schema.SchemaType.event);
    }

    public List<Schema> getAllSchemas(Schema.SchemaType schemaType) {
        return criteria()
                .add(eq("schemaType", schemaType))
                .list();
    }

    public List<Schema> getSchemasInfo(String company, String org, String namespace, String nameFilter, Schema.SchemaType schemaType, Integer offset, Integer limit) {
        Criteria criteria = criteria().createAlias("namespace", "namespace").createAlias("namespace.org","org")
                .createAlias("company", "company");
        criteria = criteria.add(Restrictions.eq("deleted",false))
                .add(Restrictions.eq("namespace.deleted", false))
                .add(Restrictions.eq("org.deleted", false));
        if(company != null && company != "*" && !company.isEmpty()){
            criteria = criteria.add(Restrictions.eq("company.name", company));
        }
        if(org != null && org != "*" && !org.isEmpty()){
            criteria = criteria.add(Restrictions.eq("org.name", org));
        }
        if(namespace != null && namespace != "*" && !namespace.isEmpty()){
            criteria = criteria.add(Restrictions.eq("namespace.name", namespace));
        }
        if(schemaType != null && schemaType != Schema.SchemaType.composite_type){
            criteria = criteria.add(Restrictions.eq("schemaType", schemaType));
        }
        else {
            criteria.add(Restrictions.ne("schemaType", Schema.SchemaType.composite_type));
        }

        return criteria.add(Restrictions.like("name", nameFilter))
                .addOrder(Order.asc("name"))
                .setFirstResult(offset.intValue()).setMaxResults(limit.intValue())
                .list();
    }

    public List<Schema> getCompositeTypeInfo(String company, String org, String namespace, String nameFilter, Integer offset, Integer limit) {
        Criteria criteria = criteria().createAlias("namespace", "namespace").createAlias("namespace.org","org")
                .createAlias("company", "company");
        criteria = criteria.add(Restrictions.eq("deleted",false))
                .add(Restrictions.eq("namespace.deleted", false))
                .add(Restrictions.eq("org.deleted", false));
        if(company != null && company != "*" && !company.isEmpty()){
            criteria = criteria.add(Restrictions.eq("company.name", company));
        }
        if(org != null && org != "*" && !org.isEmpty()){
            criteria = criteria.add(Restrictions.eq("org.name", org));
        }
        if(namespace != null && namespace != "*" && !namespace.isEmpty()){
            criteria = criteria.add(Restrictions.eq("namespace.name", namespace));
        }

        criteria.add(Restrictions.eq("schemaType", Schema.SchemaType.composite_type));

        return criteria.add(Restrictions.like("name", nameFilter))
                .setFirstResult(offset.intValue()).setMaxResults(limit.intValue())
                .list();


    }


    public List<Schema> getAllSchemas() {
        Criteria criteria = criteria().createAlias("namespace", "namespace").createAlias("namespace.org", "org")
                .createAlias("company", "company");
        return criteria.add(eq("deleted", false))
                .add(eq("namespace.deleted", false))
                .add(eq("org.deleted", false))
                .list();
    }

    public List<Schema> getAllNonCompositeSchemas() {
        Criteria criteria = criteria().createAlias("namespace", "namespace").createAlias("namespace.org", "org")
                .createAlias("company", "company");
        return criteria.add(eq("deleted", false))
                .add(eq("namespace.deleted", false))
                .add(eq("org.deleted", false))
                .add(Restrictions.ne("schemaType", Schema.SchemaType.composite_type))
                .list();
    }
    public List<Schema> getSchemas(Schema.SchemaType schemaType,String descPattern, String namePattern,Integer offset, Integer limit) {

        return criteria()
                .add(Restrictions.eq("schemaType", schemaType))
                .add(Restrictions.or(Restrictions.like("description", descPattern)
                        , Restrictions.like("name", namePattern)))
                .add(Restrictions.eq("deleted", false))
                .setFirstResult(offset)
                .setMaxResults(limit)
                .list();
    }

    /**
     *
      only works with mysql 5.6 and column should be full text search indexed.
     */
    public List<Schema> getSchemasWithFullTextSearch(String searchKey,Integer offset, Integer limit) {
// criteria query  don't support  match and AGAINST


        String query="SELECT * FROM seraph_schema"
                + " where match(description) AGAINST('"+searchKey+"')"
                + " or match(name) AGAINST('"+searchKey+"')"
                +" LIMIT "+String.valueOf(limit)
                +" OFFSET "+String.valueOf(offset);


        return currentSession().createSQLQuery(query).addEntity(Schema.class).list();
    }

    @Override
    public void deleteEntity(Schema entity) {
        entity.setDeleted(true);
        save(entity);
    }

    public int getNextVersion(String company, String org, String namespace, String name, Schema.SchemaType schemaType) {

        int nextVersion = 1;

        List<Schema> result = queryByName(company, org, namespace, name, schemaType)
                .setBoolean("deleted", true)
                .list();

        if (!result.isEmpty()){
            Ordering<Schema> byVersion = new Ordering<Schema>() {
                @Override
                public int compare(@Nullable Schema left, @Nullable Schema right) {
                    return Ints.compare(left.getSchemaVersion(), right.getSchemaVersion());
                }
            };
            Collections.sort(result, byVersion);
            nextVersion = result.get(result.size()-1).getSchemaVersion() + 1;
        }

        return nextVersion;
    }

    public List<Schema> getJiraIdForSchemaApproval(String company, String org, String namespace, String name) {
        Criteria criteria = criteria().createAlias("namespace", "namespace").createAlias("namespace.org", "org")
                .createAlias("company", "company");
        criteria = criteria.add(Restrictions.eq("company.name", company));
        criteria = criteria.add(Restrictions.eq("org.name", org));
        criteria = criteria.add(Restrictions.eq("namespace.name", namespace));
        criteria = criteria.add(Restrictions.isNotNull("jiraId"));
        return criteria.add(Restrictions.eq("name", name)).list();
    }

    public List<Schema> getSchemaByName(String company, String org, String namespace, String name) {
        Criteria criteria = criteria().createAlias("namespace", "namespace").createAlias("namespace.org", "org")
                .createAlias("company", "company");
        criteria = criteria.add(Restrictions.eq("company.name", company));
        criteria = criteria.add(Restrictions.eq("org.name", org));
        criteria = criteria.add(Restrictions.eq("namespace.name", namespace));
        return criteria.add(Restrictions.eq("name", name)).list();
    }

    public void update(Schema schema) {
        currentSession().update(schema);
        currentSession().flush();
        currentSession().refresh(schema);
    }

  /**
   *
   * @param comp
   * @param org
   * @param nameSpace
   * @param name
   * @return  Product Owner in CSV (comma separated emails)
   */
  public String getProdOwnerCSV(String comp, String org, String nameSpace, String name, Schema.SchemaType schemaType){
    return getOwners(comp, org, nameSpace, name, OwnerType.PROD_OWNER, schemaType);
  }

  /**
   *
   * @param comp
   * @param org
   * @param nameSpace
   * @param name
   * @return Eng Owner in CSV (comma separated emails)
   */
  public String getEngOwnerCSV(String comp, String org, String nameSpace, String name, Schema.SchemaType schemaType){
    return getOwners(comp, org, nameSpace, name, OwnerType.ENG_OWNER, schemaType);
  }

  public String getOwners(String comp, String org, String nameSpace, String name, OwnerType ownerType, Schema.SchemaType schemaType){
    Schema result = getSchema(comp, org, nameSpace, name, schemaType);
    if(ownerType.equals(OwnerType.ENG_OWNER))
      return result.getEngOwners();
    else
      return result.getProdOwners();
  }

  public Schema getSchema(String company, String org, String namespace, String name, Schema.SchemaType schemaType){
    Query query = currentSession().createQuery(
      "select ss from Schema ss join ss.company as com join ss.namespace as ns join ns.org as org where ss.schemaType =:schemaType and com.name = :company" +
        " and org.name=:org and ns.name=:namespace and ss.name=:name and ss.deleted =:deleted")
      .setString("company", company)
      .setString("org", org)
      .setString("namespace", namespace)
      .setString("name", name)
      .setString("schemaType", schemaType.toString())
      .setBoolean("deleted", false);
    List<Schema> schemaList = query.list();
    if (schemaList.size() > 1)
      throw new IllegalStateException("There are more than 1 entities by name: " + name);

    Schema output = schemaList.size() == 0 ? null : schemaList.iterator().next();
    return output;

  }

  /**
   *
   * @param comp
   * @param org
   * @param nameSpace
   * @return all emails for the given namespace
   */
  public String getEngOwnersByNamespace(String comp, String org, String nameSpace){
    return getOwnersByNamespace(comp, org, nameSpace, OwnerType.ENG_OWNER);
  }

  public String getProdOwnersByNamespace(String comp, String org, String nameSpace){
    return getOwnersByNamespace(comp, org, nameSpace, OwnerType.PROD_OWNER);
  }

  /**
   *
   * @param company
   * @param org
   * @param namespace
   * @param ownerType
   * @return all uniq set of owners for the given namespace.
   */
  public String getOwnersByNamespace(String company, String org, String namespace, OwnerType ownerType) {
    String entityOwners = "";
    Query query = currentSession().createQuery(
      "select ss from Schema ss join ss.company as com join ss.namespace as ns join ns.org as org where com.name = :company "
        +
        "and org.name=:org and ns.name=:namespace and ss.deleted =:deleted")
      .setString("company", company)
      .setString("org", org)
      .setString("namespace", namespace)
      .setBoolean("deleted", false);
    List<Schema> result = query.list();
    for (Schema schema : result) {
      String schemaOwners = (ownerType.equals(OwnerType.ENG_OWNER))? schema.getEngOwners() : schema.getProdOwners();
      entityOwners = (schemaOwners != null && !schemaOwners.isEmpty())?(entityOwners+schemaOwners+","):entityOwners;
    }
    entityOwners = entityOwners.substring(0, entityOwners.length() - 1); //removing last comma ','
    String[] entity = entityOwners.split(",");
    Set<String> set = new LinkedHashSet<String>(Arrays.asList(entity));
    entityOwners = "";
    for(String uniqOwner : set){
      entityOwners += uniqOwner;
      entityOwners += ",";
    }
    entityOwners = entityOwners.substring(0, entityOwners.length() - 1); //removing last comma ','
    return entityOwners;
  }


  enum OwnerType{
    ENG_OWNER,
    PROD_OWNER
  }
}
