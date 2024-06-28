package com.flipkart.fdp.superbi.http.client.qaas;

import lombok.Getter;

@Getter
public class QaasDownloadResponse {
    private final String url;
    private final String service;
    private final boolean redirect;

    public QaasDownloadResponse(String url) {
        this.redirect = true;
        this.service = "QAAS_SERVICE";
        this.url = url;
    }
}
