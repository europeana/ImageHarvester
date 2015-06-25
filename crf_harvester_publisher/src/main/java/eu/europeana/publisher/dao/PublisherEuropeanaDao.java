package eu.europeana.publisher.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.publisher.domain.HarvesterDocument;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherEuropeanaDao {
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private DB mongoDB;

    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    public PublisherEuropeanaDao (MongoConfig mongoConfig) throws UnknownHostException {

        if (null == mongoConfig) {
            throw new IllegalArgumentException ("mongoConfig cannot be null");
        }

        mongoDB = mongoConfig.connectToDB();

        final Datastore dataStore = new Morphia().createDatastore(mongoConfig.connectToMongo(), mongoConfig.getdBName());
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


        for (final SourceDocumentReferenceMetaInfo metaInfo: metaInfos) {
            final String id = metaInfo.getId();
            completeHarvesterDocuments.add(incompleteHarvesterDocuments.get(id).withSourceDocumentReferenceMetaInfo(metaInfo));
        }

        return completeHarvesterDocuments;
    }

    private Map<String, HarvesterDocument> retrieveHarvesterDocumentsWithoutMetaInfo(final DBCursor cursor, final int batchSize) {
        final Map<String, HarvesterDocument> documentStatistics = new HashMap<>();

        for (int count = 0; cursor.hasNext() && count < batchSize; ++count) {
            final BasicDBObject item = (BasicDBObject)cursor.next();
            final DateTime updatedAt = new DateTime(item.getDate("updatedAt"));
            final String sourceDocumentReferenceId = item.getString("sourceDocumentReferenceId");
            final BasicDBObject referenceOwnerTemp = (BasicDBObject) item.get("referenceOwner");

            final String providerId = referenceOwnerTemp.getString("providerId");
            final String collectionId = referenceOwnerTemp.getString("collectionId");
            final String recordId = referenceOwnerTemp.getString("recordId");
            final String executionId = referenceOwnerTemp.getString("executionId");

            documentStatistics.put(sourceDocumentReferenceId, new HarvesterDocument(sourceDocumentReferenceId, updatedAt, new ReferenceOwner(providerId,collectionId,recordId,executionId),null));
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
