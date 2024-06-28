package com.flipkart.fdp.superbi.subscription.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.reflect.ClassPath;
import java.util.HashSet;
import java.util.Set;
import lombok.SneakyThrows;

/**
 * Created by akshaya.sharma on 26/10/18
 */

public class ClassScannerUtil {
  @SneakyThrows
  public static Set<Class> getClasses(final String packageName, Class... parentClasses) {
    Preconditions.checkNotNull(packageName);
    Preconditions.checkNotNull(parentClasses);
    Preconditions.checkArgument(parentClasses.length > 0);

    Set<Class> resultSet = new HashSet<Class>();
    UnmodifiableIterator<ClassPath.ClassInfo> classInfoIterator = ClassPath.from(
        ClassScannerUtil.class.getClassLoader()).getTopLevelClassesRecursive(packageName).iterator();
    while(classInfoIterator.hasNext()) {
      Class clazz = Class.forName(classInfoIterator.next().getName());

      for(int i = 0; i < parentClasses.length; i++) {
        Class parentClass = parentClasses[i];
        if(parentClass.isAnnotation() && clazz.isAnnotationPresent(parentClass)) {
          resultSet.add(clazz);
        }else if(parentClass.isAssignableFrom(clazz)){
          resultSet.add(clazz);
        }
      }
    }
    return resultSet;
  }
}
