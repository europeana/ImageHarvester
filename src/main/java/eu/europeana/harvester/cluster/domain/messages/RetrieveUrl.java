package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.httpclient.HttpRetrieveConfig;

import java.io.Serializable;

public class RetrieveUrl implements Serializable {

    private final String url;

    private final HttpRetrieveConfig httpRetrieveConfig;

    private final String jobId;

    public RetrieveUrl(String url, HttpRetrieveConfig httpRetrieveConfig, String jobId) {
        this.url = url;
        this.httpRetrieveConfig = httpRetrieveConfig;
        this.jobId = jobId;
    }

    public String getUrl() {
        return url;
    }

    public HttpRetrieveConfig getHttpRetrieveConfig() {
        return httpRetrieveConfig;
    }

    public String getJobId() {
        return jobId;
    }
}
