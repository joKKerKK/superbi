package com.flipkart.fdp.superbi.dsl;

/**
 * User: aniruddha.gangopadhyay
 * Date: 11/03/14
 * Time: 3:01 PM
 */

import com.flipkart.fdp.superbi.dsl.evaluators.JSScriptEngineAccessor;
import java.util.Date;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

/**
 * Created with IntelliJ IDEA.
 * User: ankurg
 * Date: 12/8/13
 * Time: 3:43 PM
 * To change this template use File | Settings | File Templates.
 */

// TODO: have a better strategy for visitation
public enum DataType {
    STRING {
        @Override
        public String toValue(String value) {
            return value;
        }
    }, INTEGER {
        @Override
        public Integer toValue(String value) {
            return Integer.parseInt(value);
        }
        public Integer format(Object obj) {
            return (int)Double.parseDouble(String.valueOf(obj));
        }
    }, LONG {
        @Override
        public Long toValue(String value) {
            return Long.parseLong(value);
        }
    }, DOUBLE {
        @Override
        public Double toValue(String value) {
            return Double.parseDouble(value);
        }
    }, DATE {
        @Override
        public Date toValue(String value) {
            // Tries to parse static date, if failed tries to parse it as expression
            try {
                return dateParser.parseDateTime(value).toDate();
            } catch (Exception e) {
                return resolve(value);
            }
        }
        @Override
        public Date resolve(String userExpression) {
            return new Date(((Double) ExpEvaluators.JS.eval(userExpression)).longValue());
        }

    }, DATETIME {
        @Override
        public Date toValue(String value) {
            // Tries to parse static date, if failed tries to parse it as expression
            try {
                return dateParser.parseDateTime(value).toDate();
            } catch (Exception e) {
                return resolve(value);
            }
        }

        @Override
        public Date resolve(String userExpression) {
            try {
                return new Date(Long.valueOf(userExpression));
            } catch (Exception e) {
                return new Date(((Double) ExpEvaluators.JS.eval(userExpression)).longValue());
            }
        }

        @Override
        public String format(Object obj) {
            try {
                return new DateTime(obj).toString();
            } catch (Exception e) {
                try {
                    return dateParser.parseDateTime(String.valueOf(obj)).toString();
                }
                catch (Exception e1) {
                    return new DateTime(Long.valueOf(String.valueOf(obj))).toString();
                }
            }
        }

    }, BOOLEAN {
        @Override
        public Object toValue(String value) {
          return Boolean.parseBoolean("1".equals(value)?"true":value);
        }
    }, NULL{
        @Override
        public Object toValue(String value) {
            throw new NullPointerException();
        }
    };

    public Object format(Object obj) {
        return String.valueOf(obj);
    }

    public static enum ExpEvaluators {
        JS {
            public Object eval(String expr) {
                ScriptEngine engine = JSScriptEngineAccessor.borrowObject();
                boolean ok = true;

                try {
                    return engine.eval(expr);
                } catch (ScriptException se) {
                    ok = false;
                    throw new RuntimeException(String.format("JSEvaluation of expression '%s' failed", expr), se);
                } finally {
                    if (ok) JSScriptEngineAccessor.returnObject(engine);
                    else JSScriptEngineAccessor.invalidateObject(engine);
                }
            }
        };
        public abstract Object eval(String userExpression);
    }

    public Object resolve(String userExpression) {
        return userExpression;
    }

    public abstract Object toValue(String value);

    public static DataType from(Object value) {
        if (value == null) {
            return NULL;
        } else if (value instanceof String) {
            return STRING;
        } else if (value instanceof Integer) {
            return INTEGER;
        } else if (value instanceof Long) {
            return LONG;
        } else if (value instanceof Double || value instanceof Float) {
            return DOUBLE;
        } else if (value instanceof Date) {
            return DATETIME;
        } else if (value instanceof Boolean) {
            return BOOLEAN;
        } else {
            throw new IllegalArgumentException("No data type exists for value: " + value + " classType: " + value.getClass().getName());
        }
    }

    protected DateTimeFormatter dateParser = new DateTimeFormatterBuilder().append(null, new DateTimeParser[]{
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd").getParser()
    }).toFormatter();

    public static boolean isNumber(DataType valueType) {
        return valueType == INTEGER || valueType == LONG || valueType == DOUBLE;
    }

    public static boolean isDate(DataType valueType) {
        return valueType == DATETIME || valueType == DATE;
    }

    public static boolean isString(DataType valueType) {
        return valueType == STRING;
    }




}
