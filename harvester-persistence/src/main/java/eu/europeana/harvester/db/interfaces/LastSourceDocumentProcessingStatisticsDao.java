package eu.europeana.harvester.db.interfaces;

import com.google.code.morphia.Key;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.LastSourceDocumentProcessingStatistics;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.domain.URLSourceType;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by salexandru on 22.07.2015.
 */
public interface LastSourceDocumentProcessingStatisticsDao {

    /**
     * Counts the number of docs in the collection.
     * @return returns the number of documents in the collection.
     *
     */
    public Long getCount();

    /**
     * Persists a SourceDocumentProcessingStatistics object
     * @param lastSourceDocumentProcessingStatistics - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    boolean create(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * Reads and returns a SourceDocumentProcessingStatistics object
     * @param id the unique id of the record
     * @return - found SourceDocumentProcessingStatistics object, it can be null
     */
    LastSourceDocumentProcessingStatistics read(String id);

    LastSourceDocumentProcessingStatistics read(String sourceDocumentReferenceId, DocumentReferenceTaskType taskType, URLSourceType urlSourceType);

    /**
     * Updates a SourceDocumentProcessingStatistics record
     * @param lastSourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    boolean update(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * If the object doesn't exists creates it otherwise updates the a SourceDocumentProcessingStatistics record
     * @param lastSourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    com.google.code.morphia.Key<LastSourceDocumentProcessingStatistics> createOrModify(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics,
                                                                                       WriteConcern writeConcern);


    /**
     * If the objects don't exists they get created; otherwise updates the a SourceDocumentProcessingStatistics record
     * @param lastSourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    Iterable<com.google.code.morphia.Key<LastSourceDocumentProcessingStatistics>> createOrModify (Collection<LastSourceDocumentProcessingStatistics>
                                                                                                          lastSourceDocumentProcessingStatistics,
                                                                                              WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
    WriteResult delete(String id);

    /**
     * Searches for SourceDocumentProcessingStatistics which has referenceOwner.recordId equal with the given ID.
     * @param recordID resources record ID
     * @return - a list of SourceDocumentProcessingStatistics objects
     */
    List<LastSourceDocumentProcessingStatistics> findByRecordID(String recordID);

    /**
     *  @deprecated "This is a time consuming operation. Use it with great care!"
     *
     *  For every document that has the {@link ProcessingState} ERROR, SUCCESS or READY  count the number of documents.
     *  @return - a mapping between the {@link ProcessingState} and the number of documents that have that state
     */
    @Deprecated
    Map<ProcessingState, Long> countNumberOfDocumentsWithState();

    /**
     * @deprecated "This operation is time consuming. It does an update on the entire db"
     *
     * Returns all the jobs from the DB for a specific owner and deactivates it.
     *
     * @param sourceDocumentReferenceIds
     * @return - list of ProcessingJobs
     */
    @Deprecated
    List<LastSourceDocumentProcessingStatistics> deactivateDocuments(final List<String> sourceDocumentReferenceIds, final WriteConcern concern);
}
