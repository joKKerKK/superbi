package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Base;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;

/**
 * User: aartika
 * Date: 4/8/14
 */
public class BaseDAO<E extends Base> extends AbstractDAO<E> {


    public BaseDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public E save(E entity) {
        persist(entity);
        currentSession().flush();
        currentSession().refresh(entity);
        return entity;
    }

    public E getById(Serializable id) {
        return get(id);
    }

    public E getByName(String name) {
        E result = uniqueResult(criteria().add(Restrictions.eq("name", name)));
        if (result != null)
            currentSession().refresh(result);
        return result;
    }

    public List<E> getByFields(String[] fieldName, Object[] values) {
        Criteria criteria = criteria();
        for (int i=0; i<fieldName.length; i++) {
            criteria.add(Restrictions.eq(fieldName[i], values[i]));
        }
        List<E> result = criteria.list();

        return result;
    }

    public List<E> getByFields(Map<String, Object> map) {
        Criteria criteria = criteria();
        Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> restriction = it.next();
            criteria.add(Restrictions.eq(restriction.getKey(), restriction.getValue()));
        }

        List<E> result = criteria.list();

        return result;
    }

    public List<E> getAll() {
        return criteria().list();
    }

    public void deleteEntity(E entity) {
        currentSession().delete(entity);
        currentSession().flush();
    }

}
