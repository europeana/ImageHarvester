package eu.europeana.harvester.client;

import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.domain.report.SubTaskState;
import eu.europeana.harvester.domain.report.SubTaskType;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.joda.time.Interval;

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
     *
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
     * @param executionId The owner execution id of the processing jobs.
     * @return
     */
    Map<JobState, Long> countProcessingJobsByState(final String executionId);

    /**
     * Calculates the duration of a specific execution Id. This will give "in progress" results if the processing
     * jobs of the execution id has not finished yet.
     * @param executionId The execution id.
     * @return
     */
    public Interval getDateIntervalForProcessing(final String executionId);

    /**
     * Computes the subtask state counts for the last processing stats.
     * For example this method would be used to get the counts for the states of the METADATA_EXTRACTION subtask.
     *
     * @param executionId   The execution id of the processing jobs.
     * @param urlSourceType The url source type. If missing for all.
     * @param subtaskType   The sub task type for which to compute.
     * @return
     */
    Map<SubTaskState,Long> countSubTaskStatesByUrlSourceType(final String executionId, final URLSourceType urlSourceType, final SubTaskType subtaskType);

    /**
     * Computes the job state counts for a specific job type.
     * @param executionId The execution id of the job.
     * @param urlSourceType The url source type.
     * @param documentReferenceTaskType The task type.
     * @return
     */
    Map<ProcessingState,Long> countJobStatesByUrlSourceType(final String executionId, final URLSourceType urlSourceType, final DocumentReferenceTaskType documentReferenceTaskType);


    /**
     * Retrieves all the last job stats that match specific criteria.
     *
     * @param collectionId     The owner collection id of the processing jobs.
     * @param executionId      The execution id of the processing jobs. If missing for all.
     * @param processingStates The processing states for which to retrieve the stats. If empty or null will take all states.
     * @return
     */
    public List<LastSourceDocumentProcessingStatistics> findLastSourceDocumentProcessingStatistics(final String collectionId, final String executionId, final List<ProcessingState> processingStates);

}

