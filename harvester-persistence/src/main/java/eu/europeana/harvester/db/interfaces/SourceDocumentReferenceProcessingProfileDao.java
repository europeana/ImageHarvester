package eu.europeana.harvester.db.interfaces;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReferenceProcessingProfile;

import java.util.Collection;
import java.util.List;

/**
 * Created by salexandru on 17.07.2015.
 */
public interface SourceDocumentReferenceProcessingProfileDao {
    /**
     * Persists a SourceDocumentReferenceProcessingProfile object only if it's not created yet
     *
     * @param sourceDocumentReference - a new object
     * @param writeConcern            describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                operation
     * @return returns if the operation was successful
     */
    boolean create(SourceDocumentReferenceProcessingProfile sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Modifies an existing SourceDocumentReferenceProcessingProfile record, if it doesn't exists then creates it.
     *
     * @param sourceDocumentReference modified or new object
     * @param writeConcern            describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                operation
     */
    void createOrModify(SourceDocumentReferenceProcessingProfile sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Modifies an existing SourceDocumentReferenceProcessingProfile record, if it doesn't exists then creates it.
     *  @param sourceDocumentReferences modified or new objects
     * @param writeConcern             describes the guarantee that MongoDB provides when reporting on the success of a write
     */
    Iterable<com.google.code.morphia.Key<SourceDocumentReferenceProcessingProfile>> createOrModify(Collection<SourceDocumentReferenceProcessingProfile> sourceDocumentReferences, WriteConcern writeConcern);

    /**
     * Reads and returns a SourceDocumentReferenceProcessingProfile object
     *
     * @param id the unique id of the record
     * @return - found SourceDocumentReferenceProcessingProfile object, it can be null
     */
    SourceDocumentReferenceProcessingProfile read(String id);

    /**
     * Reads and returns a list of SourceDocumentReferenceProcessingProfile objects
     *
     * @param ids the unique ids of the records
     * @return - found SourceDocumentReferenceProcessingProfile object, it can be null
     */
    List<SourceDocumentReferenceProcessingProfile> read(List<String> ids);

    /**
     * Updates a SourceDocumentReferenceProcessingProfile record
     *
     * @param sourceDocumentReference the modified SourceDocumentReferenceProcessingProfile object
     * @param writeConcern            describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                operation
     * @return - success or failure
     */
    boolean update(SourceDocumentReferenceProcessingProfile sourceDocumentReference, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     *
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
    WriteResult delete(String id);

    /**
     * Searches for SourceDocumentReferenceProcessingProfiles which has referenceOwner.recordId equal with the given ID.
     *
     * @param recordID resources record ID
     * @return - a list of SourceDocumentReferenceProcessingProfile objects
     */
    List<SourceDocumentReferenceProcessingProfile> findByRecordID(String recordID);

    /**
     *  Find all SourceDocumentReferenceProcessingProfile which should have been re-evaluated till now
     *   (toBeEvaluatedAt < Date.now())
     *   @return - a list of SourceDocumentReferenceProcessingProfile objects
     */
    List<SourceDocumentReferenceProcessingProfile> getJobToBeEvaluated();

    /**
     * @deprecated "This operation is time consuming. It does an update on the entire db"
     *
     * Returns all the jobs from the DB for a specific owner and deactivates it.
     *
     * @param owner  filter criteria.
     * @return - list of ProcessingJobs
     */
    @Deprecated
    List<SourceDocumentReferenceProcessingProfile> deactivateDocuments (final ReferenceOwner owner);
}
