package com.flipkart.fdp.superbi.dsl.query.exp;

import com.flipkart.fdp.superbi.dsl.evaluators.JSScriptEngineAccessor;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.utils.Funcs;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;

/**
 * User: shashwat
 * Date: 22/01/14
 */
public enum ExpEvaluators implements ExpEvaluator {
    JS {

        @Override public Object eval(String expression, Collection<Param> params, Map<String, String[]> paramValues) {
            ScriptEngine engine = null;
            try {
                final Iterable<Map.Entry<String, Object>> entries = Iterables.transform(
                        params, Param.F.evalParamFunc(paramValues));

                final SimpleBindings simpleBindings = new SimpleBindings();
                simpleBindings.putAll(Funcs.Maps.toMap(entries));
                engine = JSScriptEngineAccessor.borrowObject();
                return engine.eval(expression, simpleBindings);

            } catch (Exception e) {
                throw new RuntimeException(e); // TODO: better exception handling using checked exceptions
            } finally {
                if (engine != null)
                    JSScriptEngineAccessor.returnObject(engine);
            }
        }

    }
}
