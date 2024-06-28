package com.flipkart.fdp.superbi.cosmos.data.query.budget;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by arun.khetarpal on 01/10/15.
 */
public class QueryBudgetResult {
    private String estimatorUsed;
    private EstimatedBudgetType budgetType;
    private Map<String, Object> data;

    public static class Builder {
        private QueryBudgetResult result = new QueryBudgetResult();

        public Builder setEstimatorUsed(String estimatorUsed) {
            result.estimatorUsed = estimatorUsed;
            return this;
        }

        public Builder setEstimatedBudgetType(EstimatedBudgetType estimatedBudgetType) {
            result.budgetType = estimatedBudgetType;
            return this;
        }

        public Builder setData(String key, Object value ) {
            result.data.put(key, value);
            return this;
        }

        public QueryBudgetResult build() {
            return new QueryBudgetResult(this);
        }
    }

    private QueryBudgetResult() {
        data = new HashMap<>();
    }

    private QueryBudgetResult(Builder builder) {
        QueryBudgetResult that = builder.result;
        this.budgetType = that.budgetType;
        this.data = that.data;
        this.estimatorUsed = that.estimatorUsed;
    }

    public String getEstimatorUsed() {
        return estimatorUsed;
    }

    public EstimatedBudgetType getBudgetType() {
        return budgetType;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            return toString();
        }
    }
}
