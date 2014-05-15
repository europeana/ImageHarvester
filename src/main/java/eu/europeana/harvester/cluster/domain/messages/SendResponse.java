package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;

import java.io.Serializable;

public class SendResponse implements Serializable{

    private final HttpRetrieveResponse httpRetrieveResponse;

    private final String jobId;

    public SendResponse(HttpRetrieveResponse httpRetrieveResponse, String jobId) {
        this.httpRetrieveResponse = httpRetrieveResponse;
        this.jobId = jobId;
    }

    public HttpRetrieveResponse getHttpRetrieveResponse() {
        return httpRetrieveResponse;
    }

    public String getJobId() {
        return jobId;
    }
}
