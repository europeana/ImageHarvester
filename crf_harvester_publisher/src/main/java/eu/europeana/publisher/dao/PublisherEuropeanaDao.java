package eu.europeana.publisher.dao;

import com.drew.lang.annotations.NotNull;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.*;
import eu.europeana.harvester.db.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.publisher.domain.MongoConfig;
import eu.europeana.publisher.domain.RetrievedDoc;
import eu.europeana.publisher.logic.PublisherMetrics;
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

    private PublisherMetrics metrics;
    private DB mongoDB;

    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    public PublisherEuropeanaDao (@NotNull MongoConfig mongoConfig, @NotNull PublisherMetrics metrics) throws
                                                                                                       UnknownHostException {
        final Mongo mongo = new Mongo(mongoConfig.getHost(), mongoConfig.getPort());

        if (StringUtils.isNotEmpty(mongoConfig.getdBUsername())) {
           boolean auth = mongo.getDB("admin").authenticate(mongoConfig.getdBUsername(), mongoConfig.getdBPassword().toCharArray());

            if (!auth) {
                LOG.error ("Publisher Europeana Mongo auth failed");
                System.exit(-1);
            }
        }
        mongoDB = mongo.getDB(mongoConfig.getdBName());
        this.metrics = metrics;

        final Datastore dataStore = new Morphia().createDatastore(mongo, mongoConfig.getdBName());
        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(dataStore);
    }

    public Map<String, RetrievedDoc> retrieveDocumentStatistics(final DBCursor cursor, final int batchSize) {
        final Map<String, RetrievedDoc> retrievedDocs = new HashMap<>();

        for (final DBObject dbObject: cursor.batchSize(batchSize)) {
            final BasicDBObject item = (BasicDBObject)dbObject;
            final DateTime updatedAt = new DateTime(item.getDate("updatedAt"));
            final String sourceDocumentReferenceId = item.getString("sourceDocumentReferenceId");
            final BasicDBObject referenceOwnerTemp = (BasicDBObject) item.get("referenceOwner");
            final String recordId = referenceOwnerTemp.getString("recordId");

            retrievedDocs.put(sourceDocumentReferenceId, new RetrievedDoc(sourceDocumentReferenceId, recordId, updatedAt));
        }


        return retrievedDocs;
    }

    public List<SourceDocumentReferenceMetaInfo> retrieveMetaInfo(final Collection<RetrievedDoc> retrievedDocs) {
        final List<String> ids = new ArrayList<>();

        for (final RetrievedDoc retrievedDoc: retrievedDocs) {
            ids.add(retrievedDoc.getSourceDocumentReferenceId());
        }

        return sourceDocumentReferenceMetaInfoDao.read(ids);
    }

    public DBCursor buildCursorForDocumentStatistics (final Date dateFilter) {
        final BasicDBObject findQuery = new BasicDBObject();
        final BasicDBObject retrievedFields = new BasicDBObject();

        if (null != dateFilter) {
            findQuery.put("updatedAt", new BasicDBObject("$gt", dateFilter));
        }

        retrievedFields.put("sourceDocumentReferenceId", 1);
        retrievedFields.put("httpResponseContentType", 1);
        retrievedFields.put("referenceOwner.recordId", 1);
        retrievedFields.put("updatedAt", 1);
        retrievedFields.put("_id", 0);

        return mongoDB.getCollection("SourceDocumentProcessingStatistics").find(findQuery, retrievedFields);
    }

    public DBCursor buildCursorForSourceDocumentProcessingStatistics (final Date dateFilter) {
        return null;
    }
}
