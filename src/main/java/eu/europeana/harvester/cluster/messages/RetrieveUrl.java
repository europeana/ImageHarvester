package eu.europeana.harvester.cluster.messages;

import eu.europeana.harvester.httpclient.HttpRetrieveConfig;

import java.io.Serializable;

public class RetrieveUrl implements Serializable {

    private final String url;

    private final HttpRetrieveConfig httpRetrieveConfig;

    public RetrieveUrl(String url, HttpRetrieveConfig httpRetrieveConfig) {
        this.url = url;
        this.httpRetrieveConfig = httpRetrieveConfig;
    }

    public String getUrl() {
        return url;
    }

    public HttpRetrieveConfig getHttpRetrieveConfig() {
        return httpRetrieveConfig;
    }

}
