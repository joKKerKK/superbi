package com.flipkart.fdp.superbi.brv2.factory;

import com.flipkart.fdp.superbi.cosmos.DataSourceType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankClient;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.hdfs.HdfsConfig;
import com.flipkart.fdp.superbi.refresher.dao.hdfs.HdfsDataSourceDao;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;

public class HdfsDataSourceFactory implements DataSourceFactory {

    public static final int TIMEOUT_IN_MS = 180000;
    public static final Integer MAX_CONNECTIONS = 10;
    private final CacheDao handleStore;
    private final GringottsClient gringottsClient;
    private final IronBankClient ironBankClient;

    public HdfsDataSourceFactory(CacheDao handleStore,GringottsClient gringottsClient,IronBankClient ironBankClient) {
        this.handleStore = handleStore;
        this.gringottsClient = gringottsClient;
        this.ironBankClient = ironBankClient;
    }

    @Override
    public DataSourceDao getDao(Map<String, Object> attributes) {
        String jdbcUrl = (String) attributes.get("jdbcUrl");
        String username = (String) attributes.get("username");
        String password = (String) attributes.get("password");
        String queue = (String) attributes.get("queue");
        String priorityClient = (String) attributes.get("priorityClient");
        int recoveryTimeOutLimitMs = Integer.valueOf((String) attributes.get("recoveryTimeOutLimitMs"));
        List<String> initScripts = JsonUtil.fromJson((String) attributes.get("initScripts"),List.class);
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
        return DataSourceType.HDFS.getDslConfig(dslConfig);
    }
}