package eu.europeana.harvester.db.interfaces;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
     boolean create(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * Reads and returns a SourceDocumentProcessingStatistics object
     * @param id the unique id of the record
     * @return - found SourceDocumentProcessingStatistics object, it can be null
     */
     SourceDocumentProcessingStatistics read(String id);

    /**
     * Updates a SourceDocumentProcessingStatistics record
     * @param sourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
     boolean update(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * If the object doesn't exists creates it otherwise updates the a SourceDocumentProcessingStatistics record
     * @param sourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
     com.google.code.morphia.Key<SourceDocumentProcessingStatistics> createOrModify(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * If the objects don't exists they get created; otherwise updates the a SourceDocumentProcessingStatistics record
     * @param sourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
     Iterable<com.google.code.morphia.Key<SourceDocumentProcessingStatistics>> createOrModify (Collection
                                                                                                             <SourceDocumentProcessingStatistics> sourceDocumentProcessingStatistics,
                                                                                                     WriteConcern writeConcern);

        /**
         * Deletes a record from DB
         * @param id the unique id of the record
         * @return - an object which contains all information about this operation
         */
     WriteResult delete(String id);

    /**
     * Search for a SourceDocumentProcessingStatistics object by an SourceDocumentReference and a ProcessingJob id
     * and returns it
     * @param id SourceDocumentReference id
     * @return found SourceDocumentProcessingStatistics object
     */
     SourceDocumentProcessingStatistics findBySourceDocumentReferenceAndJobId(String id, String jobId);

    /**
     * Searches for SourceDocumentProcessingStatistics which has referenceOwner.recordId equal with the given ID.
     * @param recordID resources record ID
     * @return - a list of SourceDocumentProcessingStatistics objects
     */
     List<SourceDocumentProcessingStatistics> findByRecordID(String recordID);

    /**
     *  @deprecated "This is a time consuming operation. Use it with great care!"
     *
     *  For every document that has the {@link ProcessingState} ERROR, SUCCESS or READY  count the number of documents.
     *  @return - a mapping between the {@link ProcessingState} and the number of documents that have that state
     */
    @Deprecated
    Map<ProcessingState, Long> countNumberOfDocumentsWithState();

    /**
     * @deprecated "This operation is time consuming. It does an update on the entire db"
     *
     * Returns all the jobs from the DB for a specific owner and deactivates it.
     *
     * @param sourceDocumentReferenceIds
     * @return - list of ProcessingJobs
     */
     @Deprecated
     List<SourceDocumentProcessingStatistics> deactivateDocuments(final List<String> sourceDocumentReferenceIds, final WriteConcern concern);

}
