package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;


import com.flipkart.fdp.superbi.cosmos.data.query.QuerySubmitResult;
import java.util.Optional;


/**
 * Created by arun.khetarpal on 17/09/15.
 */
public interface Executable<T extends QuerySubmitResult> {
    public long elapsedTimeMs();

    public Optional<T> execute(ExecutionContext context, NativeQueryTranslator translator);
}
