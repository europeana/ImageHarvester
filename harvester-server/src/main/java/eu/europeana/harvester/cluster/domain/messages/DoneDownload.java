package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.RetrievingState;

import java.io.Serializable;

/**
 * Message sent by slave actor and node master actor after the download is done.
 */
public class DoneDownload implements Serializable {

    /**
     * The ID of the task.
     */
    private final String taskID;

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
    private final RetrievingState retrieveState;

    /**
     * Contains different information about the task retrieved while downloading.
     */
    private final HttpRetrieveResponse httpRetrieveResponse;

    /**
     * The type of the tasks.
     * Contains:
     *  - The specific task type: check limit, conditional or unconditional download.
     *  - a list of subtasks
     */
    private final ProcessingJobTaskDocumentReference documentReferenceTask;

    private final String ipAddress;

    private final ProcessingJobSubTaskStats subTaskStats;

    public DoneDownload(final String taskID, final String url, final String referenceId, final String jobId,
                        final RetrievingState retrieveState,
                        final HttpRetrieveResponse httpRetrieveResponse,
                        final ProcessingJobTaskDocumentReference documentReferenceTask,
                        final String ipAddress,
                        final ProcessingJobSubTaskStats subTaskStats
                        ) {
        this.taskID = taskID;
        this.url = url;
        this.referenceId = referenceId;
        this.jobId = jobId;
        this.retrieveState = retrieveState;
        this.httpRetrieveResponse = httpRetrieveResponse;
        this.documentReferenceTask = documentReferenceTask;
        this.ipAddress = ipAddress;
        this.subTaskStats = subTaskStats;
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

    public HttpRetrieveResponse getHttpRetrieveResponse() {
        return httpRetrieveResponse;
    }

    public RetrievingState getRetrieveState () {
        return retrieveState;
    }

    public ProcessingJobTaskDocumentReference getDocumentReferenceTask() {
        return documentReferenceTask;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getTaskID() {
        return taskID;
    }

    public ProcessingJobSubTaskStats getSubTaskStats() {
        return subTaskStats;
    }
}
