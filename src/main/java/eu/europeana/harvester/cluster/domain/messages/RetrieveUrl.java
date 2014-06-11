package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.ResponseHeader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Message sent by the cluster master actor to the slaves when it wants to download an url.
 */
public class RetrieveUrl implements Serializable {

    /**
     * The url.
     */
    private final String url;

    /**
     * Contains all the necessary config information.
     */
    private final HttpRetrieveConfig httpRetrieveConfig;

    /**
     * The caller jobs id.
     */
    private final String jobId;

    /**
     * SourceDocumentReferenceId
     */
    private final String referenceId;

    /**
     * The HTTP response headers.
     */
    private final List<ResponseHeader> headers;

    private final DocumentReferenceTaskType documentReferenceTaskType;

    public RetrieveUrl(String url, HttpRetrieveConfig httpRetrieveConfig, String jobId, String referenceId,
                       List<ResponseHeader> headers, DocumentReferenceTaskType documentReferenceTaskType) {
        this.url = url;
        this.httpRetrieveConfig = httpRetrieveConfig;
        this.jobId = jobId;
        this.referenceId = referenceId;
        this.headers = headers;
        this.documentReferenceTaskType = documentReferenceTaskType;
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

    public List<ResponseHeader> getHeaders() {
        return headers;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public DocumentReferenceTaskType getDocumentReferenceTaskType() {
        return documentReferenceTaskType;
    }
}
