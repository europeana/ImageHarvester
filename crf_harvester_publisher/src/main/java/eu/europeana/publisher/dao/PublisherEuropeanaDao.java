package eu.europeana.publisher.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceDaoImpl;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

import com.codahale.metrics.Timer.Context;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherEuropeanaDao {
    private DB mongoDB;

    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    public PublisherEuropeanaDao (MongoConfig mongoConfig) throws UnknownHostException {

        if (null == mongoConfig) {
            throw new IllegalArgumentException ("mongoConfig cannot be null");
        }

        mongoDB = mongoConfig.connectToDB();

        final Datastore dataStore = new Morphia().createDatastore(mongoConfig.connectToMongo(), mongoConfig.getDbName());
        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(dataStore);
    }

    public List<HarvesterDocument> retrieveDocumentsWithMetaInfo (final DBCursor cursor, final int batchSize) {
        if (null == cursor) {
            throw new IllegalArgumentException ("cursor is null");
        }

        if (batchSize <= 0) {
            throw new IllegalArgumentException ("batch size should be a positive number (0 is excluded the)");
        }

        final List<HarvesterDocument> completeHarvesterDocuments = new ArrayList<>();

        final Map<String, HarvesterDocument> incompleteHarvesterDocuments = retrieveHarvesterDocumentsWithoutMetaInfo(cursor, batchSize);
        final List<SourceDocumentReferenceMetaInfo> metaInfos = retrieveMetaInfo(incompleteHarvesterDocuments.keySet());

        PublisherMetrics.Publisher.Read.Mongo.totalNumberOfDocumentsStatistics.inc(incompleteHarvesterDocuments.size());
        PublisherMetrics.Publisher.Read.Mongo.totalNumberOfDocumentsMetaInfo.inc(metaInfos.size());


        for (final SourceDocumentReferenceMetaInfo metaInfo: metaInfos) {
            final String id = metaInfo.getId();
            completeHarvesterDocuments.add(incompleteHarvesterDocuments.remove(id).withSourceDocumentReferenceMetaInfo(metaInfo));
        }

        completeHarvesterDocuments.addAll(incompleteHarvesterDocuments.values());
        return completeHarvesterDocuments;
    }

    private Map<String, HarvesterDocument> retrieveHarvesterDocumentsWithoutMetaInfo(final DBCursor cursor, final int batchSize) {
        final Context context = PublisherMetrics.Publisher.Read.Mongo.mongoGetDocStatisticsDuration.time();
        try {
            final Map<String, HarvesterDocument> documentStatistics = new HashMap<>();

            for (int count = 0; cursor.hasNext() && count < batchSize; ++count) {
                final BasicDBObject item = (BasicDBObject) cursor.next();
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

                documentStatistics.put(sourceDocumentReferenceId,
                                       new HarvesterDocument(sourceDocumentReferenceId, updatedAt,
                                                             new ReferenceOwner(providerId, collectionId, recordId, executionId),
                                                             null, subTaskStats,
                                                             urlSourceType,
                                                             taskType,
                                                             readUrl(sourceDocumentReferenceId)
                                                            )
                                      );
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

    private String readUrl (final String sourceDocumentReferenceId) {
        if (StringUtils.isBlank(sourceDocumentReferenceId)) return null;
        final BasicDBObject findQuery = new BasicDBObject();
        final BasicDBObject retrievedFields = new BasicDBObject();

        findQuery.put("_id", sourceDocumentReferenceId);
        retrievedFields.put("url", 1);
        return (String)mongoDB.getCollection("SourceDocumentReference").findOne(findQuery).get("url");
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

    public DBCursor buildCursorForDocumentStatistics (final DateTime dateFilter) {
        final BasicDBObject findQuery = new BasicDBObject();
        final BasicDBObject retrievedFields = new BasicDBObject();

        if (null != dateFilter) {
            findQuery.put("updatedAt", new BasicDBObject("$gt", dateFilter.toDate()));
        }

        retrievedFields.put("sourceDocumentReferenceId", 1);
        retrievedFields.put("referenceOwner.recordId", 1);
        retrievedFields.put("processingJobSubTaskStats", 1);
        retrievedFields.put("urlSourceType", 1);
        retrievedFields.put("taskType", 1);
        retrievedFields.put("updatedAt", 1);
        retrievedFields.put("_id", 0);

        return mongoDB.getCollection("SourceDocumentProcessingStatistics").find(findQuery, retrievedFields);
    }
}
