package eu.europeana.harvester.client;

import com.mongodb.DBCursor;
import eu.europeana.harvester.client.report.SubTaskType;
import eu.europeana.harvester.client.report.UrlSourceTypeWithProcessingJobSubTaskStateCounts;
import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * The public interface to the eu.europeana.harvester client. Used by any external system to control the eu.europeana.harvester cluster.
 */
public interface HarvesterClient {

    /**
     * Writes to the database a list if sourceDocumentReference objects if they does not exists otherwise updates them.
     *
     * @param sourceDocumentReferences contains basic information about a source document
     */
    Iterable<com.google.code.morphia.Key<SourceDocumentReference>> createOrModifySourceDocumentReference(final
                                                                                                         Collection<SourceDocumentReference> sourceDocumentReferences) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException;


    Iterable<com.google.code.morphia.Key<SourceDocumentReferenceProcessingProfile>>
    createOrModifyProcessingProfiles(final Collection<SourceDocumentReferenceProcessingProfile> profiles);

    void
    createOrModifyProcessingJobTuples(final Collection<ProcessingJobTuple> jobTuples) throws InterruptedException,
            MalformedURLException,
            TimeoutException,
            ExecutionException,
            UnknownHostException;


    /**
     * Sends to the master a new processing job.
     *
     * @param processingJobs contains a list of jobs
     * @return the processing job
     */
    Iterable<com.google.code.morphia.Key<ProcessingJob>> createOrModify(Collection<ProcessingJob> processingJobs);

    /**
     * Sends to the master a new processing job.
     *
     * @param processingJob contains a single job
     * @return the processing job
     */
    com.google.code.morphia.Key<ProcessingJob> createOrModify(ProcessingJob processingJob);

    /**
     * Stops a working or standby job.
     *
     * @param jobId the unique id of the job
     * @return the updated job
     */
    ProcessingJob stopJob(String jobId);

    /**
     * Starts a stopped job.
     *
     * @param jobId the unique id of the job
     * @return the updated job
     */
    ProcessingJob startJob(String jobId);


    /**
     * Retrieves a job.
     *
     * @param jobId the unique id of the job
     * @return the job
     */
    ProcessingJob retrieveProcessingJob(String jobId);

    /**
     * Retrieves a historical job.
     *
     * @param jobId the unique id of the job
     * @return the job
     */
    HistoricalProcessingJob retrieveHistoricalProcessingJob(String jobId);

    /**
     * Not implemented yet.
     */
    List<ProcessingJob> findJobsByCollectionAndState(String collectionId, List<ProcessingState> state) throws Exception;

    SourceDocumentReference retrieveSourceDocumentReferenceByUrl(String url, String recordId);

    SourceDocumentReference retrieveSourceDocumentReferenceById(String id);

    /**
     * Returns all the source document reference documents with ids.
     * @param id The ids.
     * @return
     */
    List<SourceDocumentReference> retrieveSourceDocumentReferencesByIds(List<String> id);

    SourceDocumentReferenceMetaInfo retrieveMetaInfoByUrl(String url);

    void setActive(String recordID, Boolean active) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException;

    void updateSourceDocumentProcesssingStatistics(final String sourceDocumentReferenceId, final String processingJobId);

    SourceDocumentProcessingStatistics readSourceDocumentProcesssingStatistics(final String sourceDocumentReferenceId, final String processingJobId);

    /**
     * Updates specific sourceDocumentReferenceMetaInfo document.
     *
     * @param sourceDocumentReferenceMetaInfo
     * @return
     */
    boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo);

    List<ProcessingJob> deactivateJobs(final ReferenceOwner owner);

    /**
     * Computes the processing state count for the processing jobs.
     *
     * @param collectionId The owner collection id of the processing jobs.
     * @return
     */
    Map<JobState, Long> countProcessingJobsByState(final String collectionId);

    /**
     * Computes the subtask state counts for the last processing stats.
     * @param collectionId The owner collection id of the processing jobs.
     * @param urlSourceType The url source type. If missing for all.
     * @param subtaskType The sub task type for which to compute.
     * @return
     */
    Map<URLSourceType, UrlSourceTypeWithProcessingJobSubTaskStateCounts> countSubTaskStatesByUrlSourceType(final String collectionId, final URLSourceType urlSourceType,final SubTaskType subtaskType);

    /**
     * Computes the DB cursor to retrieve all the last job stats.
     * @param collectionId The owner collection id of the processing jobs.
     * @param executionId The execution id of the processing jobs. If missing for all.
     * @param batchSize The page size of the cursor.
     * @param processingStates The processing states for which to retrieve the stats.
     * @return
     */
    DBCursor findLastSourceDocumentProcessingStatistics(final String collectionId,final String executionId,final int batchSize,List<ProcessingState> processingStates);

    /**
     * Uses an existing DB cursor to retrieve a batch of last job stats.
     * @param cursor The DB cursor.
     * @return the stats
     */
    List<LastSourceDocumentProcessingStatistics> retrieveLastSourceDocumentProcessingStatistics(final DBCursor cursor);

}

