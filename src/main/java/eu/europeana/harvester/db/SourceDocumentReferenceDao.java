package eu.europeana.harvester.db;

import com.mongodb.WriteConcern;
import eu.europeana.harvester.domain.SourceDocumentReference;

/**
 * DAO for CRUD with source_document_reference collection
 */
public interface SourceDocumentReferenceDao {

    /**
     * Persists a SourceDocumentReference object only if it's not created yet
     * @param sourceDocumentReference - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    public boolean create(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Modifies an existing SourceDocumentReference record, if it doesn't exists then creates it.
     * @param sourceDocumentReference modified or new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     */
    public void createOrModify(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Reads and returns a SourceDocumentReference object
     * @param id the unique id of the record
     * @return - found SourceDocumentReference object, it can be null
     */
    public SourceDocumentReference read(String id);

    /**
     * Updates a SourceDocumentReference record
     * @param sourceDocumentReference the modified SourceDocumentReference object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean update(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     * @param sourceDocumentReference the unnecessary object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean delete(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Search for a SourceDocumentReference object by an id and returns it
     * @param url http url
     * @return found MachineResourceReference object
     */
    public SourceDocumentReference findByUrl(String url);

}
