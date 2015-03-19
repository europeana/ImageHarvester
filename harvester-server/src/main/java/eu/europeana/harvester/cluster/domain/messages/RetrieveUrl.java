package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * Message sent by the cluster master actor to the slaves when it wants to download an url.
 */
public class RetrieveUrl implements Serializable {

    private final String id;

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
    private final Map<String, String> headers;

    /**
     * The type of the tasks.
     */
    private final ProcessingJobTaskDocumentReference documentReferenceTask;

    private final String ipAddress;

    public RetrieveUrl(final String id, final String url, final HttpRetrieveConfig httpRetrieveConfig, final String jobId,
                       final String referenceId, final Map<String, String> headers,
                       final ProcessingJobTaskDocumentReference documentReferenceTask, String ipAddress) {
        this.id = id;
        this.url = url;
        this.httpRetrieveConfig = httpRetrieveConfig;
        this.jobId = jobId;
        this.referenceId = referenceId;
        this.headers = headers;
        this.documentReferenceTask = documentReferenceTask;
        this.ipAddress = ipAddress;
    }

    public RetrieveUrl(final String url, final HttpRetrieveConfig httpRetrieveConfig, final String jobId,
                       final String referenceId, final Map<String, String> headers,
                       final ProcessingJobTaskDocumentReference documentReferenceTask, String ipAddress) {
        this.id = UUID.randomUUID().toString();
        this.url = url;
        this.httpRetrieveConfig = httpRetrieveConfig;
        this.jobId = jobId;
        this.referenceId = referenceId;
        this.headers = headers;
        this.documentReferenceTask = documentReferenceTask;
        this.ipAddress = ipAddress;
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

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public ProcessingJobTaskDocumentReference getDocumentReferenceTask() {
        return documentReferenceTask;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
                append(jobId).
                append(referenceId).
                append(httpRetrieveConfig.getTaskType()).
                toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RetrieveUrl))
            return false;
        if (obj == this)
            return true;

        final RetrieveUrl otherRetrieve = (RetrieveUrl) obj;

        return new EqualsBuilder().
                append(jobId, otherRetrieve.getJobId()).
                append(referenceId, otherRetrieve.getReferenceId()).
                isEquals();
    }

    public String getId() {
        return id;
    }

}
