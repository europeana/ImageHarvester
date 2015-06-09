package eu.europeana.harvester.db;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;

import java.util.List;

/**
 * DAO for CRUD with source_document_processing_stats collection
 */
public interface SourceDocumentProcessingStatisticsDao {

    /**
     * Persists a SourceDocumentProcessingStatistics object
     * @param sourceDocumentProcessingStatistics - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    public boolean create(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * Reads and returns a SourceDocumentProcessingStatistics object
     * @param id the unique id of the record
     * @return - found SourceDocumentProcessingStatistics object, it can be null
     */
    public SourceDocumentProcessingStatistics read(String id);

    /**
     * Updates a SourceDocumentProcessingStatistics record
     * @param sourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean update(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * If the object doesn't exists creates it otherwise updates the a SourceDocumentProcessingStatistics record
     * @param sourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public void createOrModify(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * If the objects don't exists they get created; otherwise updates the a SourceDocumentProcessingStatistics record
     * @param sourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public Iterable<com.google.code.morphia.Key<SourceDocumentProcessingStatistics>> createOrUpdate(List<SourceDocumentProcessingStatistics> sourceDocumentProcessingStatistics, WriteConcern writeConcern);

        /**
         * Deletes a record from DB
         * @param id the unique id of the record
         * @return - an object which contains all information about this operation
         */
    public WriteResult delete(String id);

    /**
     * Search for a SourceDocumentProcessingStatistics object by an SourceDocumentReference and a ProcessingJob id
     * and returns it
     * @param id SourceDocumentReference id
     * @return found SourceDocumentProcessingStatistics object
     */
    public SourceDocumentProcessingStatistics findBySourceDocumentReferenceAndJobId(String id, String jobId);

    /**
     * Searches for SourceDocumentProcessingStatistics which has referenceOwner.recordId equal with the given ID.
     * @param recordID resources record ID
     * @return - a list of SourceDocumentProcessingStatistics objects
     */
    public List<SourceDocumentProcessingStatistics> findByRecordID(String recordID);
}
