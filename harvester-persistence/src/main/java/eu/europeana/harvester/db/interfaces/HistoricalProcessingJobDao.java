package eu.europeana.harvester.db.interfaces;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * DAO for CRUD with historical_processing_job collection
 */
public interface HistoricalProcessingJobDao {

    /**
     * Persists a HistoricalProcessingJob object
     *
     * @param historicalProcessingJob - a new object
     * @param writeConcern  describes the guarantee that MongoDB provides when reporting on the success of a write
     *                      operation
     * @return returns if the operation was successful
     */
     boolean create(HistoricalProcessingJob historicalProcessingJob, WriteConcern writeConcern);

    /**
     * Creates or (if existing) modifies the HistoricalProcessingJobs objects
     *
     * @param historicalProcessingJob - a new object
     * @param writeConcern   describes the guarantee that MongoDB provides when reporting on the success of a write
     *                       operation
     * @return returns if the operation was successful
     */
     com.google.code.morphia.Key<HistoricalProcessingJob> createOrModify(HistoricalProcessingJob historicalProcessingJob, WriteConcern writeConcern);

    /**
     * Creates or (if existing) modifies the HistoricalProcessingJobs objects
     *
     * @param historicalProcessingJobs - a new object
     * @param writeConcern   describes the guarantee that MongoDB provides when reporting on the success of a write
     *                       operation
     * @return returns if the operation was successful
     */
     Iterable<com.google.code.morphia.Key<HistoricalProcessingJob>> createOrModify(Collection<HistoricalProcessingJob> historicalProcessingJobs, WriteConcern writeConcern);

    /**
     * Reads and returns a HistoricalProcessingJob object
     *
     * @param id the unique id of the record
     * @return - found ProcessingJob object, it can be null
     */
    HistoricalProcessingJob read(String id);

    /**
     * Updates a ProcessingJob record
     *
     * @param processingJob the modified ProcessingJob
     * @param writeConcern  describes the guarantee that MongoDB provides when reporting on the success of a write
     *                      operation
     * @return - success or failure
     */
     boolean update(HistoricalProcessingJob processingJob, WriteConcern writeConcern);

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
     List<HistoricalProcessingJob> getJobsWithState(JobState jobState, Page page);

     /**
      * @deprecated "This operation is time consuming. It does an update on the entire db"
      *
      * Returns all the jobs from the DB for a specific owner and deactivates it.
      *
      * @param owner  filter criteria.
      * @return - list of ProcessingJobs
      */
     @Deprecated
     List<HistoricalProcessingJob> deactivateJobs(final ReferenceOwner owner, final WriteConcern writeConcern);
}
