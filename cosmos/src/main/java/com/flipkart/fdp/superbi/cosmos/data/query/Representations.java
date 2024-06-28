package com.flipkart.fdp.superbi.cosmos.data.query;

/**
 * Created by akshaya.sharma on 19/06/19
 */

import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.Representation;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.factory.DSQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.factory.HasFactoryMethod;
import com.flipkart.fdp.superbi.dsl.utils.ReflectionUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.Reflection;
import java.util.List;
import java.util.Set;
import org.codehaus.janino.ScriptEvaluator;

/**
 * User: shashwat
 * Date: 24/01/14
 */
public enum Representations implements Representation {
  JAVA {
    private String[] imports = constructImport(Reflection.getPackageName(DSQuery.class)).toArray(new String[0]);
    private Object[] emptyObjectArr = new Object[0];

    @Override
    public <T> T from(String representation, Class<T> klass) {
      final ScriptEvaluator evaluator = newEvaluator(klass);
      try {
        evaluator.cook("return " + representation + ";");
        return klass.cast(evaluator.evaluate(emptyObjectArr));
      } catch (Exception e) {
        throw new RuntimeException(e); // TODO: create and throw RepresentationParserException
      }
    }

    private <T> ScriptEvaluator newEvaluator(Class<T> klass) {
      final ScriptEvaluator evaluator = new ScriptEvaluator();
      evaluator.setDefaultImports(imports);
      evaluator.setReturnType(klass);
      return evaluator;
    }

    private List<String> constructImport(final String basePackage) {
      final Set<String> packageNames = Sets.newHashSet();
      final Set<String> staticImports = Sets.newHashSet(
          DataType.class.getName(),
          Predicate.Type.class.getName(),
          OrderByExp.Type.class.getName(),
          AggregationType.class.getName());

      // collect and dedup package and classNames
      new ReflectionUtil.PackageScanner(basePackage).scan(new ReflectionUtil.ScanCallback() {
        @Override
        public void apply(Class klass) {
          packageNames.add(Reflection.getPackageName(klass.getName()));
          if (klass.getAnnotation(HasFactoryMethod.class) != null) {
            staticImports.add(klass.getName());
          }
        }
      });

      final List<String> imports = Lists.newArrayListWithCapacity(packageNames.size() + staticImports.size());
      for (String packageName : packageNames) {
        imports.add(packageName + ".*");
      }

      for (String importClass : staticImports) {
        imports.add("static " + importClass + ".*");
      }

      imports.add(DataType.class.getName());

//            Logger.info("Loaded imports for JAVA representation:\n" + Joiner.on("\n").join(imports));
      return imports;
    }
  };

  public static DSQuery from(String representation) {
    return JAVA.from(representation, DSQueryBuilder.class).build();
  }
}
