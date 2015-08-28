package eu.europeana.publisher.dao;

import com.codahale.metrics.Timer;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.*;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.domain.HarvesterRecord;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

public class PublisherEuropeanaDao {
    private final DB mongoDB;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    public PublisherEuropeanaDao (MongoConfig mongoConfig) throws UnknownHostException {

        if (null == mongoConfig) {
            throw new IllegalArgumentException ("mongoConfig cannot be null");
        }

        mongoDB = mongoConfig.connectToDB();

        final Datastore dataStore = new Morphia().createDatastore(mongoConfig.connectToMongo(), mongoConfig.getDbName());
        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(dataStore);
    }

    private Map<String, String> retrieveUrls (Collection<String> sourceDocumentReferenceIds, final String publishingBatchId) {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId),
                 "Retrieving url information for #" + sourceDocumentReferenceIds.size());
        final Timer.Context context = PublisherMetrics.Publisher.Read.Mongo.mongoGetUrlsDuration.time();
        try {
            if (null == sourceDocumentReferenceIds || sourceDocumentReferenceIds.isEmpty()) return Collections.EMPTY_MAP;
            final BasicDBObject query = new BasicDBObject();
            final BasicDBObject keys = new BasicDBObject();

            query.put("_id", new BasicDBObject("$in", sourceDocumentReferenceIds));
            keys.put("_id", 1);
            keys.put("url", 1);

            final DBCursor cursor = mongoDB.getCollection("SourceDocumentReference")
                                           .find(query, keys)
                                           .addOption(Bytes.QUERYOPTION_NOTIMEOUT);

            final Map<String, String> urlMap = new HashMap<>(sourceDocumentReferenceIds.size());
            while (cursor.hasNext()) {
                final BasicDBObject object = (BasicDBObject) cursor.next();
                urlMap.put(object.getString("_id"), object.getString("url"));
            }

            return urlMap;
        }
        finally {
            context.stop();
        }
    }

    private Map<String, HarvesterDocument> readingStatistics (final DBCursor cursor, final String publishingBatchId) {
        if (null == cursor) {
            throw new IllegalArgumentException();
        }
        final String collectionName = cursor.getCollection().getName();
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId),
                 "Starting retrieving and processing of Statistics documents from collection: " + collectionName);
        //raw documents extracted from the db
        final Map<String, HarvesterDocument> documents = new HashMap<>();

        //contains documents with url and metainfo
        final Map<String, HarvesterDocument> harvesterDocuments = new HashMap<>();
        final List<String> sourceDocumentRecordIds = new LinkedList<>();

        final Timer.Context context = PublisherMetrics.Publisher.Read.Mongo.mongoGetDocStatisticsDuration.time(collectionName);
        try {
            while (cursor.hasNext()) {
                final HarvesterDocument harvesterDocument = toHarvesterDocument((BasicDBObject) cursor.next());
                documents.put(harvesterDocument.getSourceDocumentReferenceId(), harvesterDocument);

                if (ProcessingJobSubTaskState.SUCCESS == harvesterDocument.getSubTaskStats().getMetaExtractionState()) {
                    sourceDocumentRecordIds.add(harvesterDocument.getSourceDocumentReferenceId());
                }
            }
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId),
                     "Done retrieving");
        }
        finally {
            context.stop();
        }

        PublisherMetrics.Publisher.Read.Mongo.totalNumberOfDocumentsStatistics.inc(collectionName, documents.size());

        final Map<String, String> urls = retrieveUrls(sourceDocumentRecordIds, publishingBatchId);
        final List<SourceDocumentReferenceMetaInfo> metaInfos = retrieveMetaInfo(sourceDocumentRecordIds);

        for (final SourceDocumentReferenceMetaInfo metaInfo : metaInfos) {
            final String id = metaInfo.getId();
            final HarvesterDocument document = documents.remove(id);
            harvesterDocuments.put(id, document.withUrl(urls.get(id)).withSourceDocumentReferenceMetaInfo(metaInfo));
        }

        PublisherMetrics.Publisher.Read.Mongo.totalNumberOfDocumentsMetaInfo.inc(collectionName, documents.size());

        for (final Map.Entry<String, HarvesterDocument> document : documents.entrySet()) {
            harvesterDocuments.put(document.getKey(), document.getValue().withUrl(urls.get(document.getKey())));
        }

        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId),
                 "Done processing");
        return harvesterDocuments;
    }

    private DBCursor buildCursorLastStats (final Map<String, HarvesterDocument> harvesterDocuments) {
        // build LastSourceDocumentProcessingStatistics query
        final Set<String> recordIds = new HashSet<>(harvesterDocuments.size());
        final Set<String> sourceDocumentReferenceIds = harvesterDocuments.keySet();

        for (final HarvesterDocument document : harvesterDocuments.values()) {
            recordIds.add(document.getReferenceOwner().getRecordId());
        }

        final BasicDBObject query = new BasicDBObject();
        final BasicDBObject retrievedFields = new BasicDBObject();

        /*
         * basically ignore the record we already have
         */
        query.put("referenceOwner.recordId", new BasicDBObject("$in", recordIds));
        query.put("sourceDocumentReferenceId", new BasicDBObject("$nin", sourceDocumentReferenceIds));
        final BasicDBList orList = new BasicDBList();

        orList.add(new BasicDBObject("state", ProcessingState.ERROR.name()));
        orList.add(new BasicDBObject("state", ProcessingState.FAILED.name()));
        orList.add(new BasicDBObject("state", ProcessingState.SUCCESS.name()));

        query.put("$or", orList);

        retrievedFields.put("sourceDocumentReferenceId", 1);
        retrievedFields.put("referenceOwner", 1);
        retrievedFields.put("processingJobSubTaskStats", 1);
        retrievedFields.put("urlSourceType", 1);
        retrievedFields.put("taskType", 1);
        retrievedFields.put("_id", 0);

        return mongoDB.getCollection("LastSourceDocumentProcessingStatistics")
                      .find(query, retrievedFields)
                      .addOption(Bytes.QUERYOPTION_NOTIMEOUT);
    }

    public List<HarvesterRecord> retrieveDocuments (final DBCursor cursor, final String publishingBatchId) {
        if (null == cursor) {
            throw new IllegalArgumentException("cursor cannot be null");
        }
        final Map <String, HarvesterDocument> harvesterDocuments = readingStatistics(cursor, publishingBatchId);

        harvesterDocuments.putAll(readingStatistics(buildCursorLastStats(harvesterDocuments), publishingBatchId));

        final Map<String, List<HarvesterDocument>> groupByRecordId = new HashMap<>();

        for (final HarvesterDocument document: harvesterDocuments.values()) {
            final String recordId = document.getReferenceOwner().getRecordId();
            if (!groupByRecordId.containsKey(recordId)) {
                groupByRecordId.put(recordId, new ArrayList<HarvesterDocument>());
            }
            groupByRecordId.get(recordId).add(document);
        }

        final List<HarvesterRecord> records = new ArrayList<>();
        for (final Map.Entry<String, List<HarvesterDocument>> entry: groupByRecordId.entrySet()) {
            HarvesterRecord record = new HarvesterRecord();

            final List<HarvesterDocument> hasViewDocuments = new ArrayList<>();

            for (final HarvesterDocument document: entry.getValue()) {
                if (URLSourceType.HASVIEW == document.getUrlSourceType())
                    hasViewDocuments.add(document);
                else  record = record.with(document.getUrlSourceType(), document);
            }

            records.add(record.withHasViewDocuments(hasViewDocuments));
        }

        return records;
    }

    /**
     *  @deprecated "This is a time consuming operation. Use it with great care!"
     *
     *  @param dateFilter -- the date to filter the documents. If null returns the number of documents from the
     *                    SourceDocumentProcessingStatistics collection
     *  @return - the number of documents for which updatedAt < dateFilter
     */
    @Deprecated
    public long countNumberOfDocumentUpdatedBefore(final DateTime dateFilter) {
        final BasicDBObject findQuery = new BasicDBObject();

        if (null != dateFilter) {
            findQuery.put("updatedAt", new BasicDBObject("$gt", dateFilter.toDate()));
        }

        return mongoDB.getCollection("SourceDocumentProcessingStatistics").count(findQuery);
    }

    public List<SourceDocumentReferenceMetaInfo> retrieveMetaInfo(final Collection<String> sourceDocumentReferenceIds) {
        if (null == sourceDocumentReferenceIds || sourceDocumentReferenceIds.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA),
                 "Retrieving metainfo for #" + sourceDocumentReferenceIds.size());
        final Timer.Context context = PublisherMetrics.Publisher.Read.Mongo.mongoGetMetaInfoDuration.time();
        try {
            if (null == sourceDocumentReferenceIds || sourceDocumentReferenceIds.isEmpty()) {
                return Collections.EMPTY_LIST;
            }
            return sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceIds);
        }
        finally {
            context.close();
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA),
                     "Done retrieving");
        }
    }

    public DBCursor buildCursorForDocumentStatistics (final int batchSize, final DateTime dateFilter) {
        final BasicDBObject findQuery = new BasicDBObject();
        final BasicDBObject retrievedFields = new BasicDBObject();

        if (null != dateFilter) {
            findQuery.put("updatedAt", new BasicDBObject("$gt", dateFilter.toDate()));
        }

        final BasicDBList orList = new BasicDBList();

        orList.add(new BasicDBObject("state", ProcessingState.ERROR.name()));
        orList.add(new BasicDBObject("state", ProcessingState.FAILED.name()));
        orList.add(new BasicDBObject("state", ProcessingState.SUCCESS.name()));

        findQuery.put("$or", orList);

        retrievedFields.put("sourceDocumentReferenceId", 1);
        retrievedFields.put("referenceOwner", 1);
        retrievedFields.put("processingJobSubTaskStats", 1);
        retrievedFields.put("urlSourceType", 1);
        retrievedFields.put("taskType", 1);
        retrievedFields.put("updatedAt", 1);
        retrievedFields.put("_id", 0);

        final DBCursor cursor = mongoDB.getCollection("SourceDocumentProcessingStatistics")
                                       .find(findQuery, retrievedFields)
                                       .sort(new BasicDBObject("updatedAt", 1))
                                       .limit(batchSize);

        return cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
    }

    private final HarvesterDocument toHarvesterDocument (final BasicDBObject item) {
        final DateTime updatedAt = new DateTime(item.getDate("updatedAt"));
        final String sourceDocumentReferenceId = item.getString("sourceDocumentReferenceId");
        final BasicDBObject referenceOwnerTemp = (BasicDBObject) item.get("referenceOwner");

        final String providerId = referenceOwnerTemp.getString("providerId");
        final String collectionId = referenceOwnerTemp.getString("collectionId");
        final String recordId = referenceOwnerTemp.getString("recordId");
        final String executionId = referenceOwnerTemp.getString("executionId");

        final BasicDBObject subTaskStatsTemp = (BasicDBObject) item.get("processingJobSubTaskStats");

        final ProcessingJobSubTaskStats subTaskStats = new ProcessingJobSubTaskStats(
             ProcessingJobRetrieveSubTaskState.valueOf(subTaskStatsTemp.getString("retrieveState")),
             ProcessingJobSubTaskState.valueOf(subTaskStatsTemp.getString("colorExtractionState")),
             ProcessingJobSubTaskState.valueOf(subTaskStatsTemp.getString("metaExtractionState")),
             ProcessingJobSubTaskState.valueOf(subTaskStatsTemp.getString("thumbnailGenerationState")),
             ProcessingJobSubTaskState.valueOf(subTaskStatsTemp.getString("thumbnailStorageState"))
        );

        final DocumentReferenceTaskType taskType = DocumentReferenceTaskType.valueOf(item.getString("taskType"));

        final URLSourceType urlSourceType = URLSourceType.valueOf(item.getString("urlSourceType"));

        return new HarvesterDocument(sourceDocumentReferenceId,
                                     updatedAt,
                                     new ReferenceOwner(providerId, collectionId, recordId, executionId),
                                     null,
                                     subTaskStats,
                                     urlSourceType,
                                     taskType
        );
    }

}
