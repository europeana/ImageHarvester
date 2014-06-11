package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.ProcessingJob;

import java.util.List;

/**
 * DAO for CRUD with processing_job collection
 */
public interface ProcessingJobDao {

    /**
     * Persists a ProcessingJob object
     * @param processingJob - a new object
     */
    public void create(ProcessingJob processingJob);

    /**
     * Reads and returns a ProcessingJob object
     * @param id the unique id of the record
     * @return - found ProcessingJob object, it can be null
     */
    public ProcessingJob read(String id);

    /**
     * Updates a ProcessingJob record
     * @param processingJob the modified ProcessingJob
     * @return - success or failure
     */
    public boolean update(ProcessingJob processingJob);

    /**
     * Deletes a record from DB
     * @param processingJob the unnecessary object
     * @return - success or failure
     */
    public boolean delete(ProcessingJob processingJob);

    /**
     * Returns all the jobs from the DB
     * @return - list of ProcessingJobs
     */
    public List<ProcessingJob> getAllJobs();

}
