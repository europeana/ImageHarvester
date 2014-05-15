package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.SourceDocumentReference;

/**
 * DAO for CRUD with source_document_reference collection
 */
public interface SourceDocumentReferenceDao {

    public void create(SourceDocumentReference sourceDocumentReference);

    public SourceDocumentReference read(String id);

    public boolean update(SourceDocumentReference sourceDocumentReference);

    public boolean delete(SourceDocumentReference sourceDocumentReference);

    public void createOrModify(SourceDocumentReference sourceDocumentReference);

    public SourceDocumentReference findByUrl(String url);

}
