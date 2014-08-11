package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.cluster.domain.JobConfigs;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;

import java.io.Serializable;

/**
 * Message sent by slave actor and node master actor after the download is done.
 */
public class DoneDownload implements Serializable {

    /**
     * The url.
     */
    private final String url;

    /**
     * SourceDocumentReferenceId
     */
    private final String referenceId;

    /**
     * The caller jobs id.
     */
    private final String jobId;

    /**
     * The state of the task.
     */
    private final ProcessingState processingState;

    /**
     * The specific task type: check limit, conditional or unconditional download.
     */
    private final DocumentReferenceTaskType taskType;

    /**
     * An object which contains different configs for different types of tasks.
     */
    private final JobConfigs jobConfigs;

    /**
     * Contains different information about the task retrieved while downloading.
     */
    private final HttpRetrieveResponse httpRetrieveResponse;

    public DoneDownload(final String url, final String referenceId, final String jobId,
                        final ProcessingState processingState, final DocumentReferenceTaskType taskType,
                        final JobConfigs jobConfigs, final HttpRetrieveResponse httpRetrieveResponse) {
        this.url = url;
        this.referenceId = referenceId;
        this.jobId = jobId;
        this.processingState = processingState;
        this.taskType = taskType;
        this.jobConfigs = jobConfigs;
        this.httpRetrieveResponse = httpRetrieveResponse;
    }

    public String getUrl() {
        return url;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public String getJobId() {
        return jobId;
    }

    public DocumentReferenceTaskType getTaskType() {
        return taskType;
    }

    public JobConfigs getJobConfigs() {
        return jobConfigs;
    }

    public HttpRetrieveResponse getHttpRetrieveResponse() {
        return httpRetrieveResponse;
    }

    public ProcessingState getProcessingState() {
        return processingState;
    }
}
