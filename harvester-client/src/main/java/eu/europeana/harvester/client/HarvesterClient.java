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
     *
     * @param sourceDocumentReferences contains basic information about a source document
     */
    public Iterable<com.google.code.morphia.Key<SourceDocumentReference>> createOrModifySourceDocumentReference(final List<SourceDocumentReference> sourceDocumentReferences) throws MalformedURLException, UnknownHostException;

    /**
     * Sends to the master a new processing job.
     *
     * @param processingJobs contains a list of jobs
     * @return the processing job
     */
    public Iterable<com.google.code.morphia.Key<ProcessingJob>> createOrModify(List<ProcessingJob> processingJobs);

    /**
     * Sends to the master a new processing job.
     *
     * @param processingJob contains a single job
     * @return the processing job
     */
    public com.google.code.morphia.Key<ProcessingJob> createOrModify(ProcessingJob processingJob);

    /**
     * Stops a working or standby job.
     *
     * @param jobId the unique id of the job
     * @return the updated job
     */
    public ProcessingJob stopJob(String jobId);

    /**
     * Starts a stopped job.
     *
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
     *
     * @param jobId the unique id of the job
     * @return a ProcessingJobStats object which contains the states of tasks in different forms.
     */
    public ProcessingJobStats statsOfJob(String jobId);

    public SourceDocumentReferenceMetaInfo retrieveMetaInfoByUrl(String url);

    public void setActive(String recordID, Boolean active) throws MalformedURLException, UnknownHostException;

    public void updateSourceDocumentProcesssingStatisticsForUrl(String url);

    /**
     * Updates specific sourceDocumentReferenceMetaInfo document.
     *
     * @param sourceDocumentReferenceMetaInfo
     * @return
     */
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo);
}

