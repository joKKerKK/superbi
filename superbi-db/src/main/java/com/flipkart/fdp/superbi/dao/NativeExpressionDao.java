package com.flipkart.fdp.superbi.dao;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.superbi.entities.NativeExpression;

public interface NativeExpressionDao extends GenericDAO<NativeExpression, NativeExpression.IdClass> {
  String getNativeExpression(String nativeExpressionName , String factName);
}
