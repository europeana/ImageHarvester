package eu.europeana.harvester.client;

import eu.europeana.harvester.domain.*;

import java.util.List;

/**
 * The public interface to the eu.europeana.harvester client. Used by any external system to control the eu.europeana.harvester cluster.
 */
interface HarvesterClient {

    /**
     * Writes to the database a linkCheckLimits object if it does not exists otherwise updates it.
     * @param linkCheckLimits contains different limits for link check tasks
     */
    public void createOrModifyLinkCheckLimits(final LinkCheckLimits linkCheckLimits);

    /**
     * Writes to the database a processingLimits object if it does not exists otherwise updates it.
     * @param processingLimits contains some basic information about a server (e.g.: ip, limits, ...)
     */
    public void createOrModifyProcessingLimits(final MachineResourceReference processingLimits);

    /**
     * Writes to the database a list if sourceDocumentReference objects if they does not exists otherwise updates them.
     * @param sourceDocumentReference contains basic information about a source document
     */
    public void createOrModifySourceDocumentReference(final List<SourceDocumentReference> sourceDocumentReference);

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

    public void setActive(String recordID, Boolean active);

    /**
     * Updates specific sourceDocumentReferenceMetaInfo document.
     * @param sourceDocumentReferenceMetaInfo
     * @return
     */
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo);
}

