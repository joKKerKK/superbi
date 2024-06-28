package com.flipkart.fdp.superbi.cosmos.data.api.execution.scriptEngines;

import java.util.Map;

/**
 * Created by arun.khetarpal on 18/09/15.
 */
public interface ScriptExecutionStrategy {
    public Object execute(String alias, String expr, Map<String, Object> varBinding);
}
