package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import com.google.common.collect.Sets;
import java.util.Set;

/**
 * User: aniruddha.gangopadhyay
 * Date: 27/02/14
 * Time: 10:53 PM
 */
public class SourceTypeDAO  {

    public Set<SourceType> getSourceTypes(){
        return Sets.newHashSet(SourceType.values());
    }
}
