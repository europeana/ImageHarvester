package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingJobLimits;
import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;
import eu.europeana.harvester.domain.ReferenceOwner;
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
     * Contains all the retrieval & processing limits.
     */
    private final ProcessingJobLimits limits;

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

    /**
     * The specific task type: check limit, conditional or unconditional download.
     */
    private final DocumentReferenceTaskType taskType;

    /**
     * The owner references of the url of the task. Important for logging & monitoring purposes.
     */
    private final ReferenceOwner referenceOwner;

    public RetrieveUrl(final String id, final String url, final DocumentReferenceTaskType taskType, final ProcessingJobLimits limits, final String jobId,
                       final String referenceId, final Map<String, String> headers,
                       final ProcessingJobTaskDocumentReference documentReferenceTask, String ipAddress, final ReferenceOwner referenceOwner) {
        this.id = id;
        this.url = url;
        this.taskType = taskType;
        this.limits = limits;
        this.jobId = jobId;
        this.referenceId = referenceId;
        this.headers = headers;
        this.documentReferenceTask = documentReferenceTask;
        this.ipAddress = ipAddress;
        this.referenceOwner = referenceOwner;
    }

    public RetrieveUrl(final String url, final ProcessingJobLimits limits, DocumentReferenceTaskType taskType, final String jobId,
                       final String referenceId, final Map<String, String> headers,
                       final ProcessingJobTaskDocumentReference documentReferenceTask, String ipAddress, final ReferenceOwner referenceOwner) {
        this.id = UUID.randomUUID().toString();
        this.url = url;
        this.limits = limits;
        this.taskType = taskType;
        this.jobId = jobId;
        this.referenceId = referenceId;
        this.headers = headers;
        this.documentReferenceTask = documentReferenceTask;
        this.ipAddress = ipAddress;
        this.referenceOwner = referenceOwner;
    }

    public String getUrl() {
        return url;
    }

    public ProcessingJobLimits getLimits() {
        return limits;
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
                append(taskType).
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

    public DocumentReferenceTaskType getTaskType() {
        return taskType;
    }

    public ReferenceOwner getReferenceOwner() {
        return referenceOwner;
    }

    public RetrieveUrl withTaskType(final DocumentReferenceTaskType newTaskType) {
        return new RetrieveUrl(url, limits, newTaskType, jobId, referenceId, headers, documentReferenceTask, ipAddress, referenceOwner);
    }
}
