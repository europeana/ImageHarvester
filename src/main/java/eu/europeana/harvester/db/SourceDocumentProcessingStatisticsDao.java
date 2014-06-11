package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;

/**
 * DAO for CRUD with source_document_processing_stats collection
 */
public interface SourceDocumentProcessingStatisticsDao {

    /**
     * Persists a SourceDocumentProcessingStatistics object
     * @param sourceDocumentProcessingStatistics - a new object
     */
    public void create(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics);

    /**
     * Reads and returns a SourceDocumentProcessingStatistics object
     * @param id the unique id of the record
     * @return - found SourceDocumentProcessingStatistics object, it can be null
     */
    public SourceDocumentProcessingStatistics read(String id);

    /**
     * Updates a SourceDocumentProcessingStatistics record
     * @param sourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @return - success or failure
     */
    public boolean update(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics);

    /**
     * Deletes a record from DB
     * @param sourceDocumentProcessingStatistics the unnecessary object
     * @return - success or failure
     */
    public boolean delete(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics);

    /**
     * Search for a SourceDocumentProcessingStatistics object by an SourceDocumentReference and a ProcessingJob id
     * and returns it
     * @param id SourceDocumentReference id
     * @return found SourceDocumentProcessingStatistics object
     */
    public SourceDocumentProcessingStatistics findBySourceDocumentReferenceAndJobId(String id, String jobId);

}
