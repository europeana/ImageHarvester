package eu.europeana.publisher.dao;

import com.codahale.metrics.Timer;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
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

    public PublisherHarvesterDao (DBTargetConfig config) throws UnknownHostException {

        if (null == config || null == config.getMongoConfig()) {
            throw new IllegalArgumentException ("mongoConfig cannot be null");
        }

        final MongoConfig mongoConfig = config.getMongoConfig();
        this.connectionId = config.getName();
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
                webResourceMetaInfos.add(new WebResourceMetaInfo(document.getSourceDocumentReferenceMetaInfo().getId(),
                                                                 document.getSourceDocumentReferenceMetaInfo()
                                                                         .getImageMetaInfo(), document.getSourceDocumentReferenceMetaInfo()
                                                                                                      .getAudioMetaInfo(),
                                                                 document.getSourceDocumentReferenceMetaInfo()
                                                                         .getVideoMetaInfo(), document.getSourceDocumentReferenceMetaInfo()
                                                                                                      .getTextMetaInfo()));
            }

            PublisherMetrics.Publisher.Write.Mongo.totalNumberOfDocumentsWritten.inc(webResourceMetaInfos.size());
            webResourceMetaInfoDao.createOrModify(webResourceMetaInfos, WriteConcern.ACKNOWLEDGED);
        }
        finally {
            context.close();
        }
    }
}
