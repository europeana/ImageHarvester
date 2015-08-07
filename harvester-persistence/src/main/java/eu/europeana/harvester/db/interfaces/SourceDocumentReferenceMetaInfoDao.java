package eu.europeana.harvester.db.interfaces;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;

import java.util.Collection;
import java.util.List;

/**
 * DAO for CRUD with SourceDocumentReferenceMetaInfo collection
 */
public interface SourceDocumentReferenceMetaInfoDao {

    /**
     * Counts the number of docs in the collection.
     * @return returns the number of documents in the collection.
     *
     */
    public Long getCount();

    /**
     * Persists a SourceDocumentReferenceMetaInfo object
     * @param sourceDocumentReferenceMetaInfo - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    public boolean create(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo, WriteConcern writeConcern);

    /**
     * Persists a SourceDocumentReferenceMetaInfo object
     * @param sourceDocumentReferenceMetaInfos - a list of new objects
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     */
    public Iterable<com.google.code.morphia.Key<SourceDocumentReferenceMetaInfo>> createOrModify(Collection
                                                                                                         <SourceDocumentReferenceMetaInfo> sourceDocumentReferenceMetaInfos, WriteConcern writeConcern);


    /**
     * Reads and returns a SourceDocumentReferenceMetaInfo object
     * @param id the unique id of the record
     * @return - found SourceDocumentReferenceMetaInfo object, it can be null
     */
    public SourceDocumentReferenceMetaInfo read(String id);

    /**
     * Reads and returns a list of SourceDocumentReference objects
     * @param ids the unique ids of the records
     * @return - found SourceDocumentReferenceMetaInfo objects
     */
    public List<SourceDocumentReferenceMetaInfo> read(Collection<String> ids);

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
