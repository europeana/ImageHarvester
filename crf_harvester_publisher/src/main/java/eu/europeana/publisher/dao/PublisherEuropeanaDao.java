package eu.europeana.publisher.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.*;
import eu.europeana.harvester.db.interfaces.LastSourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.LastSourceDocumentProcessingStatisticsDaoImpl;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.domain.HarvesterRecord;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.net.UnknownHostException;
import java.util.*;

import com.codahale.metrics.Timer.Context;
import org.slf4j.LoggerFactory;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherEuropeanaDao {
    private DB mongoDB;
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

    public List<HarvesterRecord> retrieveDocuments (final DBCursor cursor, final String publishingBatchId) {
        if (null == cursor) {
            throw new IllegalArgumentException ("cursor is null");
        }

        final List<HarvesterDocument> harvesterDocuments = new ArrayList<>();

        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId,
                                                  null, null),
                 "Retrieving SourceDocumentProcessingStatistics fields"
                );

        final Map<String, HarvesterDocument> incompleteHarvesterDocuments = retrieveHarvesterDocumentsWithoutMetaInfo(cursor);
        final Map<String, String> urls = retrieveUrls (incompleteHarvesterDocuments.keySet());

        LOG.info(LoggingComponent
                         .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId, null,
                                          null),
                 "Done retrieving SourceDocumentProcessingStatistics fields. Getting the metainfo now");

        final List<SourceDocumentReferenceMetaInfo> metaInfos = retrieveMetaInfo(incompleteHarvesterDocuments.keySet());

        LOG.info(LoggingComponent
                         .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId, null,
                                          null), "Metainfo retrieved");

        PublisherMetrics.Publisher.Read.Mongo.totalNumberOfDocumentsStatistics.inc(incompleteHarvesterDocuments.size());
        PublisherMetrics.Publisher.Read.Mongo.totalNumberOfDocumentsMetaInfo.inc(metaInfos.size());


        for (final SourceDocumentReferenceMetaInfo metaInfo: metaInfos) {
            final String id = metaInfo.getId();
            harvesterDocuments.add(incompleteHarvesterDocuments.remove(id)
                                                               .withSourceDocumentReferenceMetaInfo(metaInfo)
                                                               .withUrl(urls.get(metaInfo.getId()))
                                  );
        }

        for (final Map.Entry<String, HarvesterDocument> entry: incompleteHarvesterDocuments.entrySet()) {
            harvesterDocuments.add(entry.getValue().withUrl(urls.get(entry.getKey())));
        }

        final List<HarvesterRecord> records = new ArrayList<>();

        LOG.info(LoggingComponent
                         .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId, null,
                                          null), "Retrieving LastSourceDocumentProcessingStatistics");
        final Map<String, List<HarvesterDocument>> lastStatsHarvesterDocuments = readLastStats(harvesterDocuments);

        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId, null, null),
                 "Done retrieving LastSourceDocumentProcessingStatistics"
                );

        for (final HarvesterDocument document: harvesterDocuments) {
            final String recordId = document.getReferenceOwner().getRecordId();
            final List<HarvesterDocument> hasViewDocuments = new ArrayList<>();
            HarvesterRecord record = new HarvesterRecord();

            if (URLSourceType.HASVIEW == document.getUrlSourceType()) hasViewDocuments.add(document);
            else  record = record.with(document.getUrlSourceType(), document);

            for (final HarvesterDocument lastStatsDocument: lastStatsHarvesterDocuments.get(recordId)) {
                if (document.getSourceDocumentReferenceId().equals(lastStatsDocument.getSourceDocumentReferenceId())) {
                    continue;
                }
                if (URLSourceType.HASVIEW == lastStatsDocument.getUrlSourceType())
                    hasViewDocuments.add(lastStatsDocument);
                else  record = record.with(lastStatsDocument.getUrlSourceType(), lastStatsDocument);
            }

            record = record.withHasViewDocuments(hasViewDocuments);
            records.add(record);
        }

        LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId, null, null),
                  "Done with the extra processing"
                 );
        return records;
    }

    private Map<String, String> retrieveUrls (Set<String> urls) {
        final Context context = PublisherMetrics.Publisher.Read.Mongo.mongoGetUrlsDuration.time();
        try {
            if (null == urls || urls.isEmpty()) return Collections.EMPTY_MAP;
            final BasicDBObject query = new BasicDBObject();
            final BasicDBObject keys = new BasicDBObject();

            query.put("_id", new BasicDBObject("$in", urls));
            keys.put("_id", 1);
            keys.put("url", 1);

            final DBCursor cursor = mongoDB.getCollection("SourceDocumentReference").find(query, keys)
                                           .addOption(Bytes.QUERYOPTION_NOTIMEOUT);

            final Map<String, String> urlMap = new HashMap<>();
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

    private Map<String, List<HarvesterDocument>> readLastStats (final List<HarvesterDocument> documents) {
        final Context context = PublisherMetrics.Publisher.Read.Mongo.mongoGetLastDocStatisticsDuration.time();
        try {
            if (null == documents || documents.isEmpty()) {
                return Collections.EMPTY_MAP;
            }

            final Map<String, List<HarvesterDocument>> lastStatsDocuments = new HashMap<>();
            final Set<String> recordIds = new HashSet<>(documents.size());

            for (final HarvesterDocument document : documents)
                recordIds.add(document.getReferenceOwner().getRecordId());

            final BasicDBObject query = new BasicDBObject();

            query.put("referenceOwner.recordId", new BasicDBObject("$in", recordIds));
            final BasicDBList orList = new BasicDBList();

            orList.add(new BasicDBObject("state", ProcessingState.ERROR.name()));
            orList.add(new BasicDBObject("state", ProcessingState.FAILED.name()));
            orList.add(new BasicDBObject("state", ProcessingState.SUCCESS.name()));

            query.put("$or", orList);

            final DBCursor cursor = mongoDB.getCollection("LastSourceDocumentProcessingStatistics")
                                           .find(query)
                                           .addOption(Bytes.QUERYOPTION_NOTIMEOUT);

            final Map<String, HarvesterDocument> incompleteDocuments = new HashMap<>();
            while (cursor.hasNext()) {
                final HarvesterDocument harvesterDocument = toHarvesterDocument((BasicDBObject) cursor.next());
                final String recordId = harvesterDocument.getReferenceOwner().getRecordId();

                if (!lastStatsDocuments.containsKey(harvesterDocument.getReferenceOwner().getRecordId())) {
                    lastStatsDocuments.put(recordId, new ArrayList<HarvesterDocument>());
                }

                incompleteDocuments.put(harvesterDocument.getSourceDocumentReferenceId(), harvesterDocument);
            }

            final Map<String, String> urls = retrieveUrls(incompleteDocuments.keySet());
            final List<SourceDocumentReferenceMetaInfo> metaInfos = retrieveMetaInfo(incompleteDocuments.keySet());

            for (final SourceDocumentReferenceMetaInfo metaInfo: metaInfos) {
                final HarvesterDocument document = incompleteDocuments.remove(metaInfo.getId())
                                                                      .withSourceDocumentReferenceMetaInfo(metaInfo)
                                                                      .withUrl(urls.get(metaInfo.getId()));
                final String recordId = document.getReferenceOwner().getRecordId();

                if (!lastStatsDocuments.containsKey(recordId)) {
                   lastStatsDocuments.put(recordId, new ArrayList<HarvesterDocument>());
                }
                lastStatsDocuments.get(recordId).add(document);
            }

            for (final Map.Entry<String, HarvesterDocument> entry: incompleteDocuments.entrySet()) {
                if (!lastStatsDocuments.containsKey(entry.getKey())) {
                    lastStatsDocuments.put(entry.getKey(), new ArrayList<HarvesterDocument>());
                }
                lastStatsDocuments.get(entry.getKey()).add(entry.getValue().withUrl(urls.get(entry.getKey())));
            }

            return lastStatsDocuments;
        }
        finally {
           context.stop();
        }
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

    private Map<String, HarvesterDocument> retrieveHarvesterDocumentsWithoutMetaInfo (final DBCursor cursor) {
        final Context context = PublisherMetrics.Publisher.Read.Mongo.mongoGetDocStatisticsDuration.time();
        try {
            final Map<String, HarvesterDocument> documentStatistics = new HashMap<>();

            while (cursor.hasNext()) {
                final HarvesterDocument document = toHarvesterDocument((BasicDBObject) cursor.next());
                documentStatistics.put(document.getSourceDocumentReferenceId(), document);
            }

            return documentStatistics;
        }
        finally {
           context.close();
        }
    }

    public List<SourceDocumentReferenceMetaInfo> retrieveMetaInfo(final Collection<String> sourceDocumentReferenceIds) {
        final Context context = PublisherMetrics.Publisher.Read.Mongo.mongoGetMetaInfoDuration.time();
        try {
            if (null == sourceDocumentReferenceIds || sourceDocumentReferenceIds.isEmpty()) {
                return Collections.EMPTY_LIST;
            }
            return sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceIds);
        }
        finally {
            context.close();
        }
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

        return mongoDB.getCollection("LastSourceDocumentProcessingStatistics").count(findQuery);
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
}
