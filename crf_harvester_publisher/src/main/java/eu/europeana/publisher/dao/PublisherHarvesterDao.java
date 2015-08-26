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
import eu.europeana.publisher.domain.HarvesterRecord;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherHarvesterDao {
    private static final int MAX_NUMBER_OF_RETRIES = 5;
    private final WebResourceMetaInfoDao webResourceMetaInfoDao;
    private final String connectionId;

    private final DB mongoDB;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

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

    public void writeMetaInfos (Collection<HarvesterRecord> records) {
        writeMetaInfos(records, "publisher-writeMetainfos-"+ Double.doubleToLongBits(Math.random() * 5000));
    }


    public void writeMetaInfos (Collection<HarvesterRecord> records, final String publishingBatchId) {
        final Timer.Context context = PublisherMetrics.Publisher.Write.Mongo.mongoWriteDocumentsDuration.time(connectionId);

        try {
            if (null == records || records.isEmpty()) {
                return;
            }

            final Map<String, WebResourceMetaInfo> webResourceMetaInfos = new HashMap<>();

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId,
                                                      null, null),
                     "Getting metainfos. Updating edmObject/edmPreview"
                    );

            boolean aggregationHasToUpdate = false;
            boolean europeanaAggregationHasToUpdate = false;
            final BulkWriteOperation bulkWriteAggregation = mongoDB.getCollection("Aggregation").initializeUnorderedBulkOperation();
            final BulkWriteOperation bulkWriteEuropeanaAggregation = mongoDB.getCollection("EuropeanaAggregation").initializeUnorderedBulkOperation();

            for (final HarvesterRecord record : records) {
                for (final SourceDocumentReferenceMetaInfo metaInfo : record.getUniqueMetainfos()) {
                    webResourceMetaInfos.put(metaInfo.getId(),
                                             new WebResourceMetaInfo(metaInfo.getId(), metaInfo.getImageMetaInfo(),
                                                                     metaInfo.getAudioMetaInfo(),
                                                                     metaInfo.getVideoMetaInfo(),
                                                                     metaInfo.getTextMetaInfo()));
                }

                if (record.updateEdmObject()) {
                    final String newUrl = record.newEdmObjectUrl();
                    final String[] aggregationIds = new String[]{"/aggregation/provider" + record.getRecordId(), "/provider/aggregation" + record.getRecordId()};

                    final String[] europeanaAggregationIds = new String[]{"/aggregation/europeana" + record.getRecordId(), "/europeana/aggregation" + record.getRecordId()};

                    for (final String id : aggregationIds) {
                        bulkWriteAggregation.find(new BasicDBObject("about:", id))
                                            .update(new BasicDBObject("$set", new BasicDBObject("edmObject", newUrl)));
                        aggregationHasToUpdate = true;
                    }

                    for (final String id : europeanaAggregationIds) {
                        bulkWriteEuropeanaAggregation.find(new BasicDBObject("about:", id))
                                                     .update(new BasicDBObject("$set",
                                                             new BasicDBObject("edmPreview", newUrl)));
                        europeanaAggregationHasToUpdate = true;
                    }
                }
            }

            if (aggregationHasToUpdate) {
                bulkWriteAggregation.execute(WriteConcern.ACKNOWLEDGED);
            }
            if (europeanaAggregationHasToUpdate) {
                bulkWriteEuropeanaAggregation.execute(WriteConcern.ACKNOWLEDGED);
            }

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId,
                                                      null, null),
                     "Updating finished. Started writing metainfo. #{} " + webResourceMetaInfos.size()
                    );

            int i = 0;
            final Timer.Context context_metainfo = PublisherMetrics.Publisher.Write.Mongo.mongoWriteMetaInfoDuration.time(connectionId);
            try {
                while (true) {
                    try {
                        webResourceMetaInfoDao.createOrModify(webResourceMetaInfos.values(), WriteConcern.ACKNOWLEDGED);
                        LOG.info(LoggingComponent
                                         .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA,
                                                          publishingBatchId, null, null),
                                 "Done updating in retry #" + i);
                        break;
                    } catch (Exception e) {
                        ++i;
                        if (i == MAX_NUMBER_OF_RETRIES) throw e;
                        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_EUROPEANA, publishingBatchId,
                                                                  null, null),
                                 "Retry updating #" + i, e
                                );
                    }
                }
            }
            finally {
               context_metainfo.close();
            }

            PublisherMetrics.Publisher.Write.Mongo.totalNumberOfDocumentsWritten.inc(webResourceMetaInfos.size());
            PublisherMetrics.Publisher.Write.Mongo.totalNumberOfDocumentsWrittenToOneConnection.inc(connectionId,
                                                                                                    webResourceMetaInfos.size()
                                                                                                   );
        }
        finally {
            context.close();
        }
    }
}
