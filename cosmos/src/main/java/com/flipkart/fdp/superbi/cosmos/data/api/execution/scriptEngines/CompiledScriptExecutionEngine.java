package com.flipkart.fdp.superbi.cosmos.data.api.execution.scriptEngines;


import com.flipkart.fdp.superbi.dsl.evaluators.JSScriptEngineAccessor;

import javax.script.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by arun.khetarpal on 18/09/15.
 */
public class CompiledScriptExecutionEngine implements ScriptExecutionStrategy, AutoCloseable {
    private Map<String, CompiledScript> compiledScripts;
    private final ScriptEngine scriptEngine;

    public CompiledScriptExecutionEngine() {
        compiledScripts = new HashMap<>();
        scriptEngine = JSScriptEngineAccessor.borrowObject();
    }

    public Object execute(String alias, String expr, Map<String, Object> varBinding) {
        CompiledScript script = getCompiledScript(alias, expr);

        Bindings bindings = scriptEngine.createBindings();
        bindings.putAll(varBinding);

        try {
            return script.eval(bindings);
        } catch(ScriptException se) {
            throw new RuntimeException("Expression evaluation failed " + expr, se);
        }
    }

    private CompiledScript getCompiledScript(String alias, String expr) {
        if (!compiledScripts.containsKey(alias))
            compileExpression(alias, expr);

        return compiledScripts.get(alias);
    }

    private void compileExpression(String alias, String expr) {
        try {
            compiledScripts.put(alias, ((Compilable) scriptEngine).compile(expr));
        } catch (ScriptException se) {
            throw new RuntimeException("Cannot compile expression " + expr, se);
        }
    }

    @Override
    public void close() {
        JSScriptEngineAccessor.returnObject(scriptEngine);
    }
}