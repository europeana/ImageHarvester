package eu.europeana.harvester.db;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.JobState;
import eu.europeana.harvester.domain.Page;
import eu.europeana.harvester.domain.ProcessingJob;

import java.util.List;
import java.util.Map;

/**
 * DAO for CRUD with processing_job collection
 */
public interface ProcessingJobDao {

    /**
     * Persists a ProcessingJob object
     *
     * @param processingJob - a new object
     * @param writeConcern  describes the guarantee that MongoDB provides when reporting on the success of a write
     *                      operation
     * @return returns if the operation was successful
     */
    public boolean create(ProcessingJob processingJob, WriteConcern writeConcern);

    /**
     * Creates or (if existing) modifies the ProcessingJobs objects
     *
     * @param processingJobs - a new object
     * @param writeConcern   describes the guarantee that MongoDB provides when reporting on the success of a write
     *                       operation
     * @return returns if the operation was successful
     */
    public com.google.code.morphia.Key<ProcessingJob> createOrModify(ProcessingJob processingJobs, WriteConcern writeConcern);

    /**
     * Creates or (if existing) modifies the ProcessingJobs objects
     *
     * @param processingJobs - a new object
     * @param writeConcern   describes the guarantee that MongoDB provides when reporting on the success of a write
     *                       operation
     * @return returns if the operation was successful
     */
    public Iterable<com.google.code.morphia.Key<ProcessingJob>> createOrModify(List<ProcessingJob> processingJobs, WriteConcern writeConcern);

    /**
     * Reads and returns a ProcessingJob object
     *
     * @param id the unique id of the record
     * @return - found ProcessingJob object, it can be null
     */
    public ProcessingJob read(String id);

    /**
     * Updates a ProcessingJob record
     *
     * @param processingJob the modified ProcessingJob
     * @param writeConcern  describes the guarantee that MongoDB provides when reporting on the success of a write
     *                      operation
     * @return - success or failure
     */
    public boolean update(ProcessingJob processingJob, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     *
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
    public WriteResult delete(String id);

    /**
     * Returns all the jobs from the DB with a specified state.
     *
     * @param jobState the specific state
     * @param page     an object which contains the number of records needed and the offset.
     * @return - list of ProcessingJobs
     */
    public List<ProcessingJob> getJobsWithState(JobState jobState, Page page);

    /**
     * @return a map which maps each IP address with the number of processingJobs from that IP address
     */
    public Map<String, Integer> getIpDistribution();

    /**
     * Returns all the jobs from the DB with a specified state, but it's careful to return jobs from different ips.
     *
     * @param jobState the specific state
     * @param page     an object which contains the number of records needed and the offset.
     * @return - list of ProcessingJobs
     */
    public List<ProcessingJob> getDiffusedJobsWithState(JobState jobState, Page page, Map<String, Integer> ipDistribution, Map<String, Boolean> ipsWithJobs);

}
