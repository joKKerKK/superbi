package com.flipkart.fdp.superbi.cosmos.data.hive;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.sql.SQLDSLConfig;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * Created by amruth.s on 31-10-2014.
 */
public class HiveDSLConfig extends SQLDSLConfig {

    @Getter
    private final Boolean isBadgeEnabled;

    // taking it as string type as DynamicRemote config has the map in the form of <String, DataSource> format
    public static final String IS_BADGER_ENABLED = "IS_BADGER_ENABLED";

    public HiveDSLConfig(Map<String, String> overrides) {
        super(overrides);
        this.isBadgeEnabled = Boolean.parseBoolean(overrides.getOrDefault(IS_BADGER_ENABLED, Boolean.FALSE.toString()));
    }

    @Override public List<? extends Object> getInitScripts() {
        return ImmutableList.<String>builder().add("add jar /usr/share/fk-bigfoot-hivejsonserde/json-serde-1.3-SNAPSHOT-jar-with-dependencies.jar").build();
    }


    public String getDateExpression(String dateColumn, String timeColumn) {
        return "cast( "+dateColumn + " as bigint) *10000 + " + timeColumn;
    }


}
