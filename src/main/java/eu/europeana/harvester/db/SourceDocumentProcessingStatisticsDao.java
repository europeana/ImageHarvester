package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;

/**
 * DAO for CRUD with source_document_processing_stats collection
 */
public interface SourceDocumentProcessingStatisticsDao {

    public void create(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics);

    public SourceDocumentProcessingStatistics read(String id);

    public boolean update(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics);

    public boolean delete(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics);

}
