package eu.europeana.publisher.dao;

import com.drew.lang.annotations.NotNull;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.*;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.publisher.domain.DocumentStatistic;
import eu.europeana.publisher.domain.MongoConfig;
import eu.europeana.publisher.domain.RetrievedDocument;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherEuropeanaDao {
    private static final Logger LOG = LogManager.getLogger(PublisherEuropeanaDao.class.getName());

    private DB mongoDB;

    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    public PublisherEuropeanaDao (@NotNull MongoConfig mongoConfig) throws UnknownHostException {

        if (null == mongoConfig) {
            throw new IllegalArgumentException ("mongoConfig cannot be null");
        }

        final Mongo mongo = new Mongo(mongoConfig.getHost(), mongoConfig.getPort());

        if (StringUtils.isNotEmpty(mongoConfig.getdBUsername())) {
           boolean auth = mongo.getDB("admin").authenticate(mongoConfig.getdBUsername(), mongoConfig.getdBPassword().toCharArray());

            if (!auth) {
                LOG.error ("Publisher Europeana Mongo auth failed");
                System.exit(-1);
            }
        }
        mongoDB = mongo.getDB(mongoConfig.getdBName());

        final Datastore dataStore = new Morphia().createDatastore(mongo, mongoConfig.getdBName());
        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(dataStore);
    }

    public List<RetrievedDocument> retrieveDocumentsWithMetaInfo (final DBCursor cursor, final int batchSize) {
        if (null == cursor) {
            throw new IllegalArgumentException ("cursor is null");
        }

        if (batchSize <= 0) {
            throw new IllegalArgumentException ("batch size should be a positive number (0 is excluded the)");
        }

        final List<RetrievedDocument> retrievedDocuments = new ArrayList<>();

        final Map<String, DocumentStatistic> documentStatistics = retrieveDocumentStatistics (cursor, batchSize);
        final List<SourceDocumentReferenceMetaInfo> metaInfos = retrieveMetaInfo(documentStatistics.keySet());


        for (final SourceDocumentReferenceMetaInfo metaInfo: metaInfos) {
            final String id = metaInfo.getId();
            retrievedDocuments.add(new RetrievedDocument(documentStatistics.get(id), metaInfo));
        }

        return retrievedDocuments;
    }

    private Map<String, DocumentStatistic> retrieveDocumentStatistics(final DBCursor cursor, final int batchSize) {
        final Map<String, DocumentStatistic> documentStatistics = new HashMap<>();

        for (int count = 0; cursor.hasNext() && count < batchSize; ++count) {
            final BasicDBObject item = (BasicDBObject)cursor.next();
            final DateTime updatedAt = new DateTime(item.getDate("updatedAt"));
            final String sourceDocumentReferenceId = item.getString("sourceDocumentReferenceId");
            final BasicDBObject referenceOwnerTemp = (BasicDBObject) item.get("referenceOwner");
            final String recordId = referenceOwnerTemp.getString("recordId");

            documentStatistics.put(sourceDocumentReferenceId, new DocumentStatistic(sourceDocumentReferenceId, recordId, updatedAt));
        }

        return documentStatistics;
    }

    public List<SourceDocumentReferenceMetaInfo> retrieveMetaInfo(final Collection<String> sourceDocumentReferenceIds) {
        if (null == sourceDocumentReferenceIds || sourceDocumentReferenceIds.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        return sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceIds);
    }

    public DBCursor buildCursorForDocumentStatistics (final DateTime dateFilter) {
        final BasicDBObject findQuery = new BasicDBObject();
        final BasicDBObject retrievedFields = new BasicDBObject();

        if (null != dateFilter) {
            findQuery.put("updatedAt", new BasicDBObject("$gt", dateFilter.toDate()));
        }

        retrievedFields.put("sourceDocumentReferenceId", 1);
        retrievedFields.put("referenceOwner.recordId", 1);
        retrievedFields.put("updatedAt", 1);
        retrievedFields.put("_id", 0);

        return mongoDB.getCollection("SourceDocumentProcessingStatistics").find(findQuery, retrievedFields);
    }
}
