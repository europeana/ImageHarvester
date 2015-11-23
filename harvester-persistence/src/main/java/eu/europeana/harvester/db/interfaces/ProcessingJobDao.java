package eu.europeana.harvester.db.interfaces;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * DAO for CRUD with processing_job collection
 */
public interface ProcessingJobDao {

    /**
     * Counts the number of docs in the collection.
     * @return returns the number of documents in the processing job
     *
     */
    public Long getCount();

    /**
     * Persists a ProcessingJob object
     *
     * @param processingJob - a new object
     * @param writeConcern  describes the guarantee that MongoDB provides when reporting on the success of a write
     *                      operation
     * @return returns if the operation was successful
     */
     boolean create(ProcessingJob processingJob, WriteConcern writeConcern);

    /**
     * Creates or (if existing) modifies the ProcessingJobs objects
     *
     * @param processingJobs - a new object
     * @param writeConcern   describes the guarantee that MongoDB provides when reporting on the success of a write
     *                       operation
     * @return returns if the operation was successful
     */
     com.google.code.morphia.Key<ProcessingJob> createOrModify(ProcessingJob processingJobs, WriteConcern writeConcern);

    /**
     * Creates or (if existing) modifies the ProcessingJobs objects
     *
     * @param processingJobs - a new object
     * @param writeConcern   describes the guarantee that MongoDB provides when reporting on the success of a write
     *                       operation
     * @return returns if the operation was successful
     */
     Iterable<com.google.code.morphia.Key<ProcessingJob>> createOrModify(Collection<ProcessingJob> processingJobs, WriteConcern writeConcern);

    /**
     * Reads and returns a ProcessingJob object
     *
     * @param id the unique id of the record
     * @return - found ProcessingJob object, it can be null
     */
     ProcessingJob read(String id);

    /**
     * Updates a ProcessingJob record
     *
     * @param processingJob the modified ProcessingJob
     * @param writeConcern  describes the guarantee that MongoDB provides when reporting on the success of a write
     *                      operation
     * @return - success or failure
     */
     boolean update(ProcessingJob processingJob, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     *
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
     WriteResult delete(String id);

    /**
     * Returns all the jobs from the DB with a specified state.
     *
     * @param jobState the specific state
     * @param page     an object which contains the number of records needed and the offset.
     * @return - list of ProcessingJobs
     */
     List<ProcessingJob> getJobsWithState(JobState jobState, Page page);

    void modifyStateOfJobsWithIds(JobState newJobState,List<String> jobIds);

    void modifyStateOfJobs(JobState oldJobState, JobState newJobState);

    /**
     * @return a map which maps each IP address with the number of processingJobs from that IP address
     */
     Map<String, Integer> getIpDistribution();

    /**
     * Returns all the jobs from the DB with a specified state, but it's careful to return jobs from different ips.
     *
     * @param jobState the specific state
     * @param page     an object which contains the number of records needed and the offset.
     * @return - list of ProcessingJobs
     */
     List<ProcessingJob> getDiffusedJobsWithState(JobPriority jobPriority, JobState jobState, Page page, Map<String, Integer> ipDistribution);


     /**
      * @deprecated "This operation is time consuming. It does an update on the entire db"
      *
      * Returns all the jobs from the DB for a specific owner and deactivates it.
      *
      * @param owner  filter criteria.
      * @return - list of ProcessingJobs
      */
     @Deprecated
     List<ProcessingJob> deactivateJobs (final ReferenceOwner owner, final WriteConcern writeConcern);

    /**
     * Groups & counts by state the all the  processing jobs that belong to a specific collection.
     * @param collectionId The collection Id.
     * @return
     */
    public Map<JobState, Long> countProcessingJobsByState(final String collectionId);

}
