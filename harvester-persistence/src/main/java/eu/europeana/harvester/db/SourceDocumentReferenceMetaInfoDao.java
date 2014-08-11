package eu.europeana.harvester.db;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;

/**
 * DAO for CRUD with source_document_reference_metainfo collection
 */
public interface SourceDocumentReferenceMetaInfoDao {

    /**
     * Persists a SourceDocumentReferenceMetaInfo object
     * @param sourceDocumentReferenceMetaInfo - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    public boolean create(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo, WriteConcern writeConcern);

    /**
     * Reads and returns a SourceDocumentReferenceMetaInfo object
     * @param id the unique id of the record
     * @return - found SourceDocumentReferenceMetaInfo object, it can be null
     */
    public SourceDocumentReferenceMetaInfo read(String id);

    /**
     * Updates a SourceDocumentReferenceMetaInfo record
     * @param sourceDocumentReferenceMetaInfo the modified SourceDocumentReferenceMetaInfo
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
    public WriteResult delete(String id);

}
