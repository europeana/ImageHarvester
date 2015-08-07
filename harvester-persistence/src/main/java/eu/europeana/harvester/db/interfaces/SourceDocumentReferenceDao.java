package eu.europeana.harvester.db.interfaces;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReference;

import java.util.Collection;
import java.util.List;

/**
 * DAO for CRUD with source_document_reference collection
 */
public interface SourceDocumentReferenceDao {

    /**
     * Counts the number of docs in the collection.
     * @return returns the number of documents in the collection.
     *
     */
    public Long getCount();

    /**
     * Persists a SourceDocumentReference object only if it's not created yet
     *
     * @param sourceDocumentReference - a new object
     * @param writeConcern            describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                operation
     * @return returns if the operation was successful
     */
     boolean create(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Modifies an existing SourceDocumentReference record, if it doesn't exists then creates it.
     *
     * @param sourceDocumentReference modified or new object
     * @param writeConcern            describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                operation
     */
     void createOrModify(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Modifies an existing SourceDocumentReference record, if it doesn't exists then creates it.
     *  @param sourceDocumentReferences modified or new objects
     * @param writeConcern             describes the guarantee that MongoDB provides when reporting on the success of a write
     */
     Iterable<com.google.code.morphia.Key<SourceDocumentReference>> createOrModify(Collection
                                                                                                 <SourceDocumentReference> sourceDocumentReferences, WriteConcern writeConcern);

    /**
     * Reads and returns a SourceDocumentReference object
     *
     * @param id the unique id of the record
     * @return - found SourceDocumentReference object, it can be null
     */
     SourceDocumentReference read(String id);

    /**
     * Reads and returns a list of SourceDocumentReference objects
     *
     * @param ids the unique ids of the records
     * @return - found SourceDocumentReference object, it can be null
     */
     List<SourceDocumentReference> read(List<String> ids);

    /**
     * Updates a SourceDocumentReference record
     *
     * @param sourceDocumentReference the modified SourceDocumentReference object
     * @param writeConcern            describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                operation
     * @return - success or failure
     */
     boolean update(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     *
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
     WriteResult delete(String id);

    /**
     * Searches for SourceDocumentReferences which has referenceOwner.recordId equal with the given ID.
     *
     * @param recordID resources record ID
     * @return - a list of SourceDocumentReference objects
     */
     List<SourceDocumentReference> findByRecordID(String recordID);

    /**
     * @deprecated "This operation is time consuming. It does an update on the entire db"
     *
     * Returns all the jobs from the DB for a specific owner and deactivates it.
     *
     * @param owner  filter criteria.
     * @return - list of ProcessingJobs
     */
     @Deprecated
     List<SourceDocumentReference> deactivateDocuments (final ReferenceOwner owner, final WriteConcern concern);

}
