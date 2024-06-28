package com.flipkart.fdp.superbi.dao;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.superbi.entities.ReportFederation.IdClass;
import com.flipkart.fdp.superbi.entities.TableFederation;
import java.util.List;

/**
 * Created by mansi.jain on 04/03/22
 */
public interface TableFederationDao extends GenericDAO<TableFederation, IdClass> {

  List<TableFederation> getFederation(String fromTable, String storeIdentifier);

}
