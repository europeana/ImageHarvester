package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.cluster.domain.JobConfigs;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ResponseHeader;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;

import java.io.Serializable;
import java.util.List;

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

    /**
     * The type of the tasks.
     */
    private final DocumentReferenceTaskType documentReferenceTaskType;

    /**
     * Different configs for different types of tasks.
     */
    private final JobConfigs jobConfigs;

    public RetrieveUrl(final String url, final HttpRetrieveConfig httpRetrieveConfig, final String jobId,
                       final String referenceId, final List<ResponseHeader> headers,
                       final DocumentReferenceTaskType documentReferenceTaskType,
                       final JobConfigs jobConfigs) {
        this.url = url;
        this.httpRetrieveConfig = httpRetrieveConfig;
        this.jobId = jobId;
        this.referenceId = referenceId;
        this.headers = headers;
        this.documentReferenceTaskType = documentReferenceTaskType;
        this.jobConfigs = jobConfigs;
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

    public JobConfigs getJobConfigs() {
        return jobConfigs;
    }
}
