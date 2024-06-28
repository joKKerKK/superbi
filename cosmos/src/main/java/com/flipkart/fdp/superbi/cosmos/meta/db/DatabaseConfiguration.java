package com.flipkart.fdp.superbi.cosmos.meta.db;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Dropwizard's DatabaseConfiguration.
 *
 */
public class DatabaseConfiguration {
    @NotNull
    @JsonProperty
    private String driverClass = "com.mysql.jdbc.Driver";

    @NotNull
    @JsonProperty
    private String user = "root";

    @JsonProperty
    private String password = "";

    @NotNull
    @JsonProperty
    private String url = "jdbc:mysql://localhost:3306/test";

    @NotNull
    @JsonProperty
    private ImmutableMap<String, String> properties = ImmutableMap.of();

    @NotNull
    @JsonProperty
    private int maxWaitForConnectionMs = 1000;

    @NotNull
    @JsonProperty
    private String validationQuery = "/* Health Check */ SELECT 1";

    @Min(1)
    @Max(1024)
    @JsonProperty
    private int minSize = 1;

    @Min(1)
    @Max(1024)
    @JsonProperty
    private int maxSize = 8;

    @JsonProperty
    private boolean checkConnectionWhileIdle = false;

    @NotNull
    @JsonProperty
    private int checkConnectionHealthWhenIdleForMs = 10*1000;

    @NotNull
    @JsonProperty
    private int closeConnectionIfIdleForMs = 60*1000;

    @JsonProperty
    private boolean defaultReadOnly = false;

    @JsonProperty
    private ImmutableList<String> connectionInitializationStatements = ImmutableList.of();

    @JsonProperty
    private boolean autoCommentsEnabled = true;

    public DatabaseConfiguration() {
    }

    public boolean isAutoCommentsEnabled() {
        return autoCommentsEnabled;
    }

    public void setAutoCommentsEnabled(boolean autoCommentsEnabled) {
        this.autoCommentsEnabled = autoCommentsEnabled;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public ImmutableMap<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = ImmutableMap.copyOf(properties);
    }

    public String getValidationQuery() {
        return validationQuery;
    }

    public void setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
    }

    public int getMinSize() {
        return minSize;
    }

    public void setMinSize(int minSize) {
        this.minSize = minSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public boolean isCheckConnectionWhileIdle() {
        return checkConnectionWhileIdle;
    }

    public void setCheckConnectionWhileIdle(boolean checkConnectionWhileIdle) {
        this.checkConnectionWhileIdle = checkConnectionWhileIdle;
    }

    public boolean isDefaultReadOnly() {
        return defaultReadOnly;
    }

    public void setDefaultReadOnly(boolean defaultReadOnly) {
        this.defaultReadOnly = defaultReadOnly;
    }

    public ImmutableList<String> getConnectionInitializationStatements() {
        return connectionInitializationStatements;
    }

    public void setConnectionInitializationStatements(List<String> statements) {
        this.connectionInitializationStatements = ImmutableList.copyOf(statements);
    }

    public void setProperties(ImmutableMap<String, String> properties) {
        this.properties = properties;
    }

    public int getMaxWaitForConnectionMs() {
        return maxWaitForConnectionMs;
    }

    public void setMaxWaitForConnectionMs(int maxWaitForConnectionMs) {
        this.maxWaitForConnectionMs = maxWaitForConnectionMs;
    }

    public int getCheckConnectionHealthWhenIdleForMs() {
        return checkConnectionHealthWhenIdleForMs;
    }

    public void setCheckConnectionHealthWhenIdleForMs(int checkConnectionHealthWhenIdleForMs) {
        this.checkConnectionHealthWhenIdleForMs = checkConnectionHealthWhenIdleForMs;
    }

    public int getCloseConnectionIfIdleForMs() {
        return closeConnectionIfIdleForMs;
    }

    public void setCloseConnectionIfIdleForMs(int closeConnectionIfIdleForMs) {
        this.closeConnectionIfIdleForMs = closeConnectionIfIdleForMs;
    }

    public void setConnectionInitializationStatements(ImmutableList<String> connectionInitializationStatements) {
        this.connectionInitializationStatements = connectionInitializationStatements;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatabaseConfiguration)) return false;

        DatabaseConfiguration that = (DatabaseConfiguration) o;

        if (autoCommentsEnabled != that.autoCommentsEnabled) return false;
        if (checkConnectionHealthWhenIdleForMs != that.checkConnectionHealthWhenIdleForMs) return false;
        if (checkConnectionWhileIdle != that.checkConnectionWhileIdle) return false;
        if (closeConnectionIfIdleForMs != that.closeConnectionIfIdleForMs) return false;
        if (defaultReadOnly != that.defaultReadOnly) return false;
        if (maxSize != that.maxSize) return false;
        if (maxWaitForConnectionMs != that.maxWaitForConnectionMs) return false;
        if (minSize != that.minSize) return false;
        if (connectionInitializationStatements != null ? !connectionInitializationStatements.equals(that.connectionInitializationStatements) : that.connectionInitializationStatements != null)
            return false;
        if (!driverClass.equals(that.driverClass)) return false;
        if (password != null ? !password.equals(that.password) : that.password != null) return false;
        if (properties != null ? !properties.equals(that.properties) : that.properties != null) return false;
        if (!url.equals(that.url)) return false;
        if (!user.equals(that.user)) return false;
        if (validationQuery != null ? !validationQuery.equals(that.validationQuery) : that.validationQuery != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = driverClass.hashCode();
        result = 31 * result + user.hashCode();
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + url.hashCode();
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        result = 31 * result + maxWaitForConnectionMs;
        result = 31 * result + (validationQuery != null ? validationQuery.hashCode() : 0);
        result = 31 * result + minSize;
        result = 31 * result + maxSize;
        result = 31 * result + (checkConnectionWhileIdle ? 1 : 0);
        result = 31 * result + checkConnectionHealthWhenIdleForMs;
        result = 31 * result + closeConnectionIfIdleForMs;
        result = 31 * result + (defaultReadOnly ? 1 : 0);
        result = 31 * result + (connectionInitializationStatements != null ? connectionInitializationStatements.hashCode() : 0);
        result = 31 * result + (autoCommentsEnabled ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DatabaseConfiguration{");
        sb.append("driverClass='").append(driverClass).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append(", properties=").append(properties);
        sb.append(", maxWaitForConnectionMs=").append(maxWaitForConnectionMs);
        sb.append(", validationQuery='").append(validationQuery).append('\'');
        sb.append(", minSize=").append(minSize);
        sb.append(", maxSize=").append(maxSize);
        sb.append(", checkConnectionWhileIdle=").append(checkConnectionWhileIdle);
        sb.append(", checkConnectionHealthWhenIdleForMs=").append(checkConnectionHealthWhenIdleForMs);
        sb.append(", closeConnectionIfIdleForMs=").append(closeConnectionIfIdleForMs);
        sb.append(", defaultReadOnly=").append(defaultReadOnly);
        sb.append(", connectionInitializationStatements=").append(connectionInitializationStatements);
        sb.append(", autoCommentsEnabled=").append(autoCommentsEnabled);
        sb.append('}');
        return sb.toString();
    }
}
