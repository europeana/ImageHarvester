package eu.europeana.harvester.client;

import eu.europeana.harvester.domain.*;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * The public interface to the eu.europeana.harvester client. Used by any external system to control the eu.europeana.harvester cluster.
 */
public interface HarvesterClient {

    /**
     * Writes to the database a list if sourceDocumentReference objects if they does not exists otherwise updates them.
     * @param sourceDocumentReference contains basic information about a source document
     */
    public void createOrModifySourceDocumentReference(final List<SourceDocumentReference> sourceDocumentReference) throws MalformedURLException, UnknownHostException;

    /**
     * Sends to the master a new processing job.
     * @param processingJob contains a list of tasks
     * @return the processing job
     */
    public ProcessingJob createProcessingJob(final ProcessingJob processingJob);

    /**
     * Not implemented yet.
     */
    public ProcessingJob createProcessingJobForCollection(String collectionId, DocumentReferenceTaskType type);

    /**
     * Not implemented yet.
     */
    public ProcessingJob createProcessingJobForRecord(String recordId, DocumentReferenceTaskType type);

    /**
     * Stops a working or standby job.
     * @param jobId the unique id of the job
     * @return the updated job
     */
    public ProcessingJob stopJob(String jobId);

    /**
     * Starts a stopped job.
     * @param jobId the unique id of the job
     * @return the updated job
     */
    public ProcessingJob startJob(String jobId);

    /**
     * Not implemented yet.
     */
    public List<ProcessingJob> findJobsByCollectionAndState(String collectionId, List<ProcessingState> state);

    /**
     * Collects information about a job status.
     * @param jobId the unique id of the job
     * @return a ProcessingJobStats object which contains the states of tasks in different forms.
     */
    public ProcessingJobStats statsOfJob(String jobId);

    public SourceDocumentReferenceMetaInfo  retrieveMetaInfoByUrl(String url);

    public void setActive(String recordID, Boolean active) throws MalformedURLException, UnknownHostException;

    public void updateSourceDocumentProcesssingStatisticsForUrl(String url);

    /**
     * Updates specific sourceDocumentReferenceMetaInfo document.
     * @param sourceDocumentReferenceMetaInfo
     * @return
     */
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo);
}

