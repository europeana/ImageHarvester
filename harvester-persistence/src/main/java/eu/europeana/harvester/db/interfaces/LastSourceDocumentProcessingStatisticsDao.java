package eu.europeana.harvester.db.interfaces;

import com.google.code.morphia.Key;
import com.mongodb.DBCursor;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.domain.report.SubTaskState;
import eu.europeana.harvester.domain.report.SubTaskType;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by salexandru on 22.07.2015.
 */
public interface LastSourceDocumentProcessingStatisticsDao {

    /**
     * Counts the number of docs in the collection.
     *
     * @return returns the number of documents in the collection.
     */
    public Long getCount();

    /**
     * Persists a SourceDocumentProcessingStatistics object
     *
     * @param lastSourceDocumentProcessingStatistics - a new object
     * @param writeConcern                           describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                               operation
     * @return returns if the operation was successful
     */
    boolean create(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * Reads and returns a SourceDocumentProcessingStatistics object
     *
     * @param id the unique id of the record
     * @return - found SourceDocumentProcessingStatistics object, it can be null
     */
    LastSourceDocumentProcessingStatistics read(String id);

    LastSourceDocumentProcessingStatistics read(String sourceDocumentReferenceId, DocumentReferenceTaskType taskType, URLSourceType urlSourceType);

    /**
     * Updates a SourceDocumentProcessingStatistics record
     *
     * @param lastSourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern                           describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                               operation
     * @return - success or failure
     */
    boolean update(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics, WriteConcern writeConcern);

    /**
     * If the object doesn't exists creates it otherwise updates the a SourceDocumentProcessingStatistics record
     *
     * @param lastSourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern                           describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                               operation
     * @return - success or failure
     */
    com.google.code.morphia.Key<LastSourceDocumentProcessingStatistics> createOrModify(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics,
                                                                                       WriteConcern writeConcern);


    /**
     * If the objects don't exists they get created; otherwise updates the a SourceDocumentProcessingStatistics record
     *
     * @param lastSourceDocumentProcessingStatistics the modified SourceDocumentProcessingStatistics object
     * @param writeConcern                           describes the guarantee that MongoDB provides when reporting on the success of a write
     *                                               operation
     * @return - success or failure
     */
    Iterable<com.google.code.morphia.Key<LastSourceDocumentProcessingStatistics>> createOrModify(Collection<LastSourceDocumentProcessingStatistics>
                                                                                                         lastSourceDocumentProcessingStatistics,
                                                                                                 WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     *
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
    WriteResult delete(String id);

    /**
     * Searches for SourceDocumentProcessingStatistics which has referenceOwner.recordId equal with the given ID.
     *
     * @param recordID resources record ID
     * @return - a list of SourceDocumentProcessingStatistics objects
     */
    List<LastSourceDocumentProcessingStatistics> findByRecordID(String recordID);

    /**
     * @return - a mapping between the {@link ProcessingState} and the number of documents that have that state
     * @deprecated "This is a time consuming operation. Use it with great care!"
     * <p/>
     * For every document that has the {@link ProcessingState} ERROR, SUCCESS or READY  count the number of documents.
     */
    @Deprecated
    Map<ProcessingState, Long> countNumberOfDocumentsWithState();

    /**
     * @param sourceDocumentReferenceIds
     * @return - list of ProcessingJobs
     * @deprecated "This operation is time consuming. It does an update on the entire db"
     * <p/>
     * Returns all the jobs from the DB for a specific owner and deactivates it.
     */
    @Deprecated
    List<LastSourceDocumentProcessingStatistics> deactivateDocuments(final List<String> sourceDocumentReferenceIds, final WriteConcern concern);

    /**
     * Counts the sub task state for finished jobs from a specific collection, execution id and source type.
     *
     * @param executionId   The execution id.
     * @param urlSourceType The url source type. If missing all.
     * @param subtaskType
     * @return
     */
    public Map<SubTaskState,Long> countSubTaskStatesByUrlSourceType(final String executionId, final URLSourceType urlSourceType, final SubTaskType subtaskType);


    /**
     * Counts the sub task state for finished jobs from a specific collection, execution id and source type.
     * @param executionId   The execution id.
     * @param urlSourceType The url source type. If missing all.
     * @param documentReferenceTaskType
     * @return
     */
    public Map<ProcessingState,Long> countJobStatesByUrlSourceType(final String executionId, final URLSourceType urlSourceType, final DocumentReferenceTaskType documentReferenceTaskType);

        /**
         * Builds a list for a query that filters by several criterias.
         *
         * @param collectionId     The owner collection id.
         * @param executionId      The execution id. If missing all.
         * @param processingStates The processing states that should be retrieved. If missing or null all states.
         * @return
         */
    public List<LastSourceDocumentProcessingStatistics> findLastSourceDocumentProcessingStatistics(final String collectionId, final String executionId, final List<ProcessingState> processingStates);

    /**
     * Groups & counts by state the all the  processing jobs that belong to a specific execution id.
     * @param executionId The execution Id.
     * @return
     */
    public Map<JobState, Long> countProcessingJobsByState(final String executionId);

}
