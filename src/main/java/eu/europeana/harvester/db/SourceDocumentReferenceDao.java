package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.SourceDocumentReference;

/**
 * DAO for CRUD with source_document_reference collection
 */
public interface SourceDocumentReferenceDao {

    /**
     * Persists a SourceDocumentReference object
     * @param sourceDocumentReference - a new object
     */
    public void create(SourceDocumentReference sourceDocumentReference);

    /**
     * Reads and returns a SourceDocumentReference object
     * @param id the unique id of the record
     * @return - found SourceDocumentReference object, it can be null
     */
    public SourceDocumentReference read(String id);

    /**
     * Updates a SourceDocumentReference record
     * @param sourceDocumentReference the modified SourceDocumentReference object
     * @return - success or failure
     */
    public boolean update(SourceDocumentReference sourceDocumentReference);

    /**
     * Deletes a record from DB
     * @param sourceDocumentReference the unnecessary object
     * @return - success or failure
     */
    public boolean delete(SourceDocumentReference sourceDocumentReference);

    /**
     * Modifies an existing SourceDocumentReference record, if it doesn't exists then creates it.
     * @param sourceDocumentReference modified or new object
     */
    public void createOrModify(SourceDocumentReference sourceDocumentReference);

    /**
     * Search for a SourceDocumentReference object by an id and returns it
     * @param url http url
     * @return found MachineResourceReference object
     */
    public SourceDocumentReference findByUrl(String url);

}
