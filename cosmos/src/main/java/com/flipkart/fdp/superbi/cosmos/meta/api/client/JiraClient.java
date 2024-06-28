package com.flipkart.fdp.superbi.cosmos.meta.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.cosmos.meta.HttpClientInitializer;
import com.flipkart.fdp.superbi.cosmos.meta.util.HttpRequestUtils;
import java.io.IOException;
import java.util.HashMap;
import net.rcarz.jiraclient.BasicCredentials;
import net.rcarz.jiraclient.Field;
import net.rcarz.jiraclient.Issue;
import net.rcarz.jiraclient.JiraException;
import org.apache.http.client.HttpClient;

/**
 * Created by ankurgupta.p on 26/12/16.
 */
public class JiraClient {

    final ObjectMapper objectMapper;
    //constants
    final String ISSUE_TYPE = "issueType";
    final String PROJECT = "project";
    final String REPORTER = "reporter";
    final String JIRA_API = "api";
    final String WATCHER = "watcher";

    HttpRequestUtils httpRequestUtils;

    public JiraClient() {
        HttpClient httpClient = HttpClientInitializer.getInstance();
        httpRequestUtils = new HttpRequestUtils(httpClient);
        objectMapper = new ObjectMapper();
    }

    public String createNewTicket(HashMap<String, String> jiraParams) throws JiraException {
        BasicCredentials creds = new BasicCredentials(jiraParams.get("username"), jiraParams.get("password"));
        net.rcarz.jiraclient.JiraClient jira = new net.rcarz.jiraclient.JiraClient(jiraParams.get("url"), creds);

            Issue.FluentCreate field = jira.createIssue(jiraParams.get("project"), "Support")
                    .field(Field.SUMMARY, jiraParams.get("summary"))
                    .field(Field.DESCRIPTION, jiraParams.get("description"));
            Issue newIssue = field.execute();
            return newIssue.toString();

    }

    private void addWatcher(Object jiraId, String jiraApi, String watcher, String reporter) throws IOException {
        String watcherApi = String.format("%s%s/watchers", jiraApi, jiraId);
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Authorization", reporter);
        httpRequestUtils.doPostWithJsonPayload(watcherApi, headers, watcher);
    }

    private boolean isMetaInfo(String key) {
        if (key.equals(ISSUE_TYPE) || key.equals(PROJECT) || key.equals(JIRA_API) || key.equals(REPORTER) || key.equals(WATCHER)) {
            return true;
        }
        return false;
    }


}
