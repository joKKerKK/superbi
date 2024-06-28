package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Tag;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.TagAssociation;
import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hibernate.Query;
import org.hibernate.SessionFactory;

/**
 * Created by ankur.mishra on 11/02/15.
 */
public class TagDAO extends AbstractDAO<Tag> {

    public TagDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }


    public void save(String tagName, String user, String dataSourceName, TagAssociation.SchemaType schemaType) {
        Optional<Tag> existingTag = Optional.fromNullable(getTagByName(tagName));
        TagAssociationDAO tagAssociationDAO = new TagAssociationDAO(getSessionFactory());
        Optional<TagAssociation> existingDataSource = Optional.fromNullable(tagAssociationDAO.getDataSource(dataSourceName));

        if(existingTag.isPresent() && existingDataSource.isPresent()){
            existingTag.get().getDataSources().add(existingDataSource.get());
            existingTag.get().onUpdate();
            existingDataSource.get().getTags().add(existingTag.get());
        }
        else if(existingTag.isPresent()  && !existingDataSource.isPresent()){
            TagAssociation tagAssociation = new TagAssociation(dataSourceName, schemaType);
            existingTag.get().getDataSources().add(tagAssociation);
            existingTag.get().onUpdate();
            tagAssociation.getTags().add(existingTag.get());
            currentSession().save(tagAssociation);
        }
        else if(!existingTag.isPresent() && existingDataSource.isPresent()){
            Tag tag = new Tag(tagName, user);
            tag.getDataSources().add(existingDataSource.get());
            existingDataSource.get().getTags().add(tag);
            currentSession().save(tag);
        }
        else{
            Tag tag = new Tag(tagName, user);
            TagAssociation tagAssociation = new TagAssociation(dataSourceName, schemaType);
            tag.getDataSources().add(tagAssociation);
            tagAssociation.getTags().add(tag);
            currentSession().save(tag);
            currentSession().save(tagAssociation);
        }
    }

    public Tag getTagByName(String tagName){
        try {
            return uniqueResult(criteria().add(eq("tagName", tagName)));
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    //new API's
    //1. get all data of all tags
    public Map<String, Map<String, List<String>>> getAllData() {
        Map<String, Map<String, List<String>>> allDataMap = new HashMap<String, Map<String, List<String>>>();
        final StringBuilder hql = new StringBuilder();
        hql.append("select t.tag_name, ta.schema_type, ta.data_source_name ")
                .append("from tag as t , tag_association as ta, tag_map as tm ")
                .append("where t.id = tm.tag_id and ta.id = tm.tag_association_id");

        final Query query = currentSession().createSQLQuery(hql.toString());
        List<Object[]> dataSources= (List<Object[]>)query.list();
        for(int i=0; i<dataSources.size(); i++) {
            if(allDataMap.containsKey(dataSources.get(i)[0].toString())) {
                if(allDataMap.get(dataSources.get(i)[0].toString()).containsKey(dataSources.get(i)[1]))
                    allDataMap.get(dataSources.get(i)[0].toString()).get((dataSources.get(i)[1])).add(dataSources.get(i)[2].toString());
                else {
                    List<String> dataSourceName = new ArrayList<String>();
                    dataSourceName.add(dataSources.get(i)[2].toString());
                    allDataMap.get(dataSources.get(i)[0].toString()).put(dataSources.get(i)[1].toString(), dataSourceName);
                }
            }

            else {
                Map<String, List<String>> newDataSource = new HashMap<String, List<String>>();
                List<String> dataSourceName = new ArrayList<String>();
                dataSourceName.add(dataSources.get(i)[2].toString());
                newDataSource.put(dataSources.get(i)[1].toString(), dataSourceName);
                allDataMap.put(dataSources.get(i)[0].toString(), newDataSource);
            }
        }

        return allDataMap;
    }

    //2. update table with new set of tags associated with entity
    public void alterAssociation(String dataSourceName, String user, TagAssociation.SchemaType type, List<String> tagNames) {

        TagAssociationDAO tagAssociationDAO = new TagAssociationDAO(getSessionFactory());
        Optional<TagAssociation> tagAssociation = Optional.fromNullable(tagAssociationDAO.getDataSource(dataSourceName));
        if(tagAssociation.isPresent()) {
            
            Iterator<Tag> itr  = tagAssociation.get().getTags().iterator();
            while(itr.hasNext())
            {
                Tag t = itr.next();
                if(!tagNames.contains(t.getTagName()))
                {
                    itr.remove();
                    t.getDataSources().remove(tagAssociation.get());
                    currentSession().save(t);
                    currentSession().save(tagAssociation.get());

                }
            }
        }

        for(int i=0; i<tagNames.size(); i++) {
            Optional<Tag> tag = Optional.fromNullable(getTagByName(tagNames.get(i)));
            if(!tag.isPresent() || !(tag.get().getDataSources().contains(tagAssociation)))
                save(tagNames.get(i), user, dataSourceName, type);
        }
    }

    //3. remove all tags associated with entity
    public void removeAssociation(String dataSourceName, TagAssociation.SchemaType type) {

        TagAssociationDAO tagAssociationDAO = new TagAssociationDAO(getSessionFactory());
        Optional<TagAssociation> tagAssociation = Optional.fromNullable(tagAssociationDAO.getDataSource(dataSourceName));
        Set<Tag> tags = new HashSet<Tag>();
        if(tagAssociation.isPresent())
            tags = tagAssociation.get().getTags();

        for(Tag tag: tags){
            tag.getDataSources().remove(tagAssociation.get());
            tag.onUpdate();
        }
        tagAssociation.get().setTags(null);
    }

    //5. get all tags associated with entity

    public List<String> getAssociatedTags(String dataSourceName) {
        TagAssociationDAO tagAssociationDAO = new TagAssociationDAO(getSessionFactory());
        TagAssociation tagAssociation = tagAssociationDAO.getDataSource(dataSourceName);
        List<String> tags = new ArrayList<String>();
        Iterator<Tag> iterator = tagAssociation.getTags().iterator();
        while(iterator.hasNext()){
            Tag tag = iterator.next();
            tags.add(tag.getTagName());
        }
        return tags;
    }
}
