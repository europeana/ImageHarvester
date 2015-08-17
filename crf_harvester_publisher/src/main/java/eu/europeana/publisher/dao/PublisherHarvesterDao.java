package eu.europeana.publisher.dao;

import com.codahale.metrics.Timer;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.*;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherMetrics;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherHarvesterDao {
    private final WebResourceMetaInfoDao webResourceMetaInfoDao;
    private final String connectionId;

    private final DB mongoDB;

    public PublisherHarvesterDao (DBTargetConfig config) throws UnknownHostException {

        if (null == config || null == config.getMongoConfig()) {
            throw new IllegalArgumentException ("mongoConfig cannot be null");
        }

        final MongoConfig mongoConfig = config.getMongoConfig();
        this.connectionId = config.getName();
        this.mongoDB = mongoConfig.connectToDB();
        final Datastore dataStore = new Morphia().createDatastore(mongoConfig.connectToMongo(), mongoConfig.getDbName());
        webResourceMetaInfoDao = new WebResourceMetaInfoDaoImpl(dataStore);
    }


    public void writeMetaInfos (Collection<HarvesterDocument> documents) {
        final Timer.Context context = PublisherMetrics.Publisher.Write.Mongo.mongoWriteDocumentsDuration.time(connectionId);

        try {
            if (null == documents || documents.isEmpty()) {
                return;
            }

            final List<WebResourceMetaInfo> webResourceMetaInfos = new ArrayList<>();

            for (final HarvesterDocument document : documents) {
                if (DocumentReferenceTaskType.CHECK_LINK.equals(document.getTaskType()) ||
                    !ProcessingJobSubTaskState.SUCCESS.equals(document.getSubTaskStats().getMetaExtractionState())) {
                    continue;
                }
                webResourceMetaInfos.add(new WebResourceMetaInfo(document.getSourceDocumentReferenceMetaInfo().getId(),
                                                                 document.getSourceDocumentReferenceMetaInfo()
                                                                         .getImageMetaInfo(), document.getSourceDocumentReferenceMetaInfo()
                                                                                                      .getAudioMetaInfo(),
                                                                 document.getSourceDocumentReferenceMetaInfo()
                                                                         .getVideoMetaInfo(), document.getSourceDocumentReferenceMetaInfo()
                                                                                                      .getTextMetaInfo()));

                if (updateEdmObjectUrl(document)) {
                    final WriteResult result = updateEdmObject("/aggregation/provider" + document.getReferenceOwner().getRecordId(),
                                                               document.getUrl()
                                                              );

                    if (0 == result.getN()) {
                        updateEdmObject("/provider/aggregation" + document.getReferenceOwner().getRecordId(), document.getUrl());
                    }
                }
            }

            PublisherMetrics.Publisher.Write.Mongo.totalNumberOfDocumentsWritten.inc(webResourceMetaInfos.size());
            webResourceMetaInfoDao.createOrModify(webResourceMetaInfos, WriteConcern.ACKNOWLEDGED);
        }
        finally {
            context.close();
        }
    }

    private WriteResult updateEdmObject (final String about, final String newUrl) {
        final BasicDBObject query = new BasicDBObject("about", about);
        final BasicDBObject update = new BasicDBObject();

        update.put("$set", new BasicDBObject("edmObject", newUrl));
        return mongoDB.getCollection("Aggregation").update(query, update, false, false, WriteConcern.ACKNOWLEDGED);
    }

    private boolean updateEdmObjectUrl (HarvesterDocument crfSolrDocument) {
        if (null == crfSolrDocument || null == crfSolrDocument.getUrlSourceType() || null == crfSolrDocument.getUrl()) {
            return false;
        }
        return URLSourceType.ISSHOWNBY == crfSolrDocument.getUrlSourceType() &&
               ProcessingJobSubTaskState.SUCCESS.equals(crfSolrDocument.getSubTaskStats().getThumbnailGenerationState()) &&
               ProcessingJobRetrieveSubTaskState.SUCCESS.equals(crfSolrDocument.getSubTaskStats().getThumbnailStorageState());
    }
}
