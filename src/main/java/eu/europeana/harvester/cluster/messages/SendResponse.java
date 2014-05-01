package eu.europeana.harvester.cluster.messages;

import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;

import java.io.Serializable;

public class SendResponse implements Serializable{

    private final HttpRetrieveResponse httpRetrieveResponse;

    public SendResponse(HttpRetrieveResponse httpRetrieveResponse) {
        this.httpRetrieveResponse = httpRetrieveResponse;
    }

    public HttpRetrieveResponse getHttpRetrieveResponse() {
        return httpRetrieveResponse;
    }
}
