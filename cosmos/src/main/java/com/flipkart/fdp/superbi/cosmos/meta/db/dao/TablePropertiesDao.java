package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.TableProperties;
import com.flipkart.fdp.superbi.cosmos.meta.util.MapUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hibernate.SessionFactory;

public class TablePropertiesDao extends AbstractDAO<TableProperties> {

  public static enum PropertyKeys{TTL, granularity, col_prepend, fStreamId}

  public TablePropertiesDao(SessionFactory sessionFactory) {

        super(sessionFactory);
    }

    public void saveOrUpdate(String name, Map<String,String> properties,boolean isActive){

      validateKeys(properties.keySet());
      Map<String,String> tmpMap = (properties == null) ? new HashMap<>() : properties;
      TableProperties tableProperties = getPropertiesByName(name);
      if (tableProperties == null) {
        tableProperties = new TableProperties(name, isActive, tmpMap.toString().substring(1,
            tmpMap.toString().length()-1).replaceAll(", ", "\n"));
      }else {
        Map<String,String> propertyMap = MapUtil.stringToMap(tableProperties.getProperties());
        propertyMap.putAll(tmpMap);
        tableProperties.setProperties(propertyMap.toString().substring(1,
            propertyMap.toString().length()-1).replaceAll(", ", "\n"));
      }
      currentSession().save(tableProperties);
    }

    public List<TableProperties> getAllProperties(){

        return list(criteria().add(eq("isActive", true)));
    }

    public TableProperties getPropertiesByName(String name){

      TableProperties properties = uniqueResult(criteria().add(eq("name",name)).add(eq("isActive",true)));

        return properties;
    }

    public void validateKeys(Set<String> keys){

      for (String key : keys) {
        try {
          PropertyKeys.valueOf(key);
        }catch (IllegalArgumentException e){
          throw new IllegalArgumentException("Invalid Property : "+key);
        }
      }

  }
}
