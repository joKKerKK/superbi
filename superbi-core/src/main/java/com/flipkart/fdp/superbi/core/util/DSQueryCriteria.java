package com.flipkart.fdp.superbi.core.util;

import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.flipkart.fdp.superbi.dsl.query.predicate.EqualsPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.GreaterThanEqualPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.GreaterThanPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.InPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.LessThanEqualPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.LessThanPredicate;
import com.google.common.base.Preconditions;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 22/04/20
 */

public enum DSQueryCriteria {
  not_in {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      // TODO
      return null;
    }
  }, is_null {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      // TODO
      return null;
    }
  }, is_not_null {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      // TODO
      return null;
    }
  }, eq {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      String value = values[0];
      return new EqualsPredicate(ExprFactory.COL(columnName), ExprFactory.LIT(value));
    }
  }, neq {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      return null;
    }
  }, in {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      return new InPredicate(ExprFactory.COL(columnName),
          Arrays.asList(values).stream().map(value -> ExprFactory.LIT(value)).collect(
              Collectors.toList()));
    }
  }, lt {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      return null;
    }
  }, lte {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      return null;
    }
  }, gt {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      return null;
    }
  }, gte {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      return null;
//      String value = values[0];
//      return new GreaterThanEqualPredicate(ExprFactory.COL(columnName), ExprFactory.LIT(value));
    }
  }, like {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      return null;
    }
  }, dlt {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      String value = values[0];
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      Date dateValue;
      try {
        Long longValue = Long.parseLong(value);
        dateValue = new Date(longValue);
      } catch (NumberFormatException ex) {
        try {
          dateValue = formatter.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
      }
      return new LessThanPredicate(ExprFactory.COL(columnName),
          ExprFactory.LIT(formatter.format(dateValue)));
    }
  }, dgt {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      String value = values[0];
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      Date dateValue;
      try {
        Long longValue = Long.parseLong(value);
        dateValue = new Date(longValue);
      } catch (NumberFormatException ex) {
        try {
          dateValue = formatter.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
      }
      return new GreaterThanPredicate(ExprFactory.COL(columnName),
          ExprFactory.LIT(formatter.format(dateValue)));
    }
  }, dlte {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      String value = values[0];
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      Date dateValue;
      try {
        Long longValue = Long.parseLong(value);
        dateValue = new Date(longValue);
      } catch (NumberFormatException ex) {
        try {
          dateValue = formatter.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
      }
      return new LessThanEqualPredicate(ExprFactory.COL(columnName),
          ExprFactory.LIT(formatter.format(dateValue)));
    }
  }, dgte {
    @Override
    public Criteria _getCriteria(String columnName, String[] values) {
      String value = values[0];
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      Date dateValue;
      try {
        Long longValue = Long.parseLong(value);
        dateValue = new Date(longValue);
      } catch (NumberFormatException ex) {
        try {
          dateValue = formatter.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
      }
      return new GreaterThanEqualPredicate(ExprFactory.COL(columnName),
          ExprFactory.LIT(formatter.format(dateValue)));
    }
  };

  protected abstract Criteria _getCriteria(String columnName, String[] values);

  public Criteria getCriteria(String columnName, String[] values) {
    Preconditions.checkArgument(StringUtils.isNotBlank(columnName),
        "Column name can't be blank in criteria");
    if (values == null || values.length == 0) {
      return null;
    }
    return _getCriteria(columnName, values);
  }
}
