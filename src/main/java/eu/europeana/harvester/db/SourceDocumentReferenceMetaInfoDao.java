package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;

public interface SourceDocumentReferenceMetaInfoDao {

    /**
     * Persists a SourceDocumentReferenceMetaInfo object
     * @param sourceDocumentReferenceMetaInfo - a new object
     */
    public void create(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo);

    /**
     * Reads and returns a SourceDocumentReferenceMetaInfo object
     * @param id the unique id of the record
     * @return - found SourceDocumentReferenceMetaInfo object, it can be null
     */
    public SourceDocumentReferenceMetaInfo read(String id);

    /**
     * Updates a SourceDocumentReferenceMetaInfo record
     * @param sourceDocumentReferenceMetaInfo the modified SourceDocumentReferenceMetaInfo
     * @return - success or failure
     */
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo);

    /**
     * Deletes a record from DB
     * @param sourceDocumentReferenceMetaInfo the unnecessary object
     * @return - success or failure
     */
    public boolean delete(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo);

    /**
     * Search for a SourceDocumentReferenceMetaInfo object by a collection id and returns it
     * @param id unique SourceDocumentReference id
     * @return found SourceDocumentReferenceMetaInfo object
     */
    public SourceDocumentReferenceMetaInfo findBySourceDocumentReferenceId(String id);

}
