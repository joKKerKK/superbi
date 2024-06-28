package com.flipkart.fdp.superbi.web.factory;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.hive.HiveDSLConfig;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankClient;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.hdfs.HdfsConfig;
import com.flipkart.fdp.superbi.refresher.dao.hdfs.HdfsDataSourceDao;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import java.util.List;
import java.util.Map;

public class HdfsDataSourceFactory implements DataSourceFactory {

    public static final int TIMEOUT_IN_MS = 180000;
    public static final Integer MAX_CONNECTIONS = 10;
    private final CacheDao handleStore;
    private final GringottsClient gringottsClient;
    private final IronBankClient ironBankClient;
    private final Gson gson = new Gson();

    public HdfsDataSourceFactory(CacheDao handleStore,GringottsClient gringottsClient,IronBankClient ironBankClient) {
        this.handleStore = handleStore;
        this.gringottsClient = gringottsClient;
        this.ironBankClient = ironBankClient;
    }

    @Override
    public DataSourceDao getDao(Map<String, String> attributes) {
        String jdbcUrl = attributes.get("jdbcUrl");
        String username = attributes.get("username");
        String password = attributes.get("password");
        String queue = attributes.get("queue");
        String priorityClient = attributes.get("priorityClient");
        int recoveryTimeOutLimitMs = Integer.valueOf(attributes.get("recoveryTimeOutLimitMs"));
        List<String> initScripts = gson.fromJson(attributes.get("initScripts"),List.class);
        Preconditions.checkNotNull(jdbcUrl);
        Preconditions.checkNotNull(username);
        Preconditions.checkNotNull(password);
        Preconditions.checkNotNull(queue);
        HdfsConfig hdfsConfig = HdfsConfig.builder().username(username).password(password).jdbcUrl(jdbcUrl)
            .recoveryTimeOutLimitMs(recoveryTimeOutLimitMs).queue(queue).priorityClient(priorityClient)
            .initScripts(initScripts).build();
        return new HdfsDataSourceDao(handleStore, hdfsConfig,gringottsClient,ironBankClient);
    }

    @Override
    public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
        return new HiveDSLConfig(dslConfig);
    }
}
