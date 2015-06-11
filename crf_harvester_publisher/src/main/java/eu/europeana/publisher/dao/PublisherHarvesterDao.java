package eu.europeana.publisher.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import eu.europeana.publisher.domain.MongoConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.logging.LoggingComponent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherHarvesterDao {
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private WebResourceMetaInfoDao webResourceMetaInfoDao;

    public PublisherHarvesterDao (MongoConfig mongoConfig) throws UnknownHostException {

        if (null == mongoConfig ) {
            throw new IllegalArgumentException ("mongoConfig cannot be null");
        }

        final Mongo mongo = new Mongo(mongoConfig.getHost(), mongoConfig.getPort());

        if (StringUtils.isNotEmpty(mongoConfig.getdBUsername())) {
            boolean auth = mongo.getDB("admin").authenticate(mongoConfig.getdBUsername(), mongoConfig.getdBPassword().toCharArray());

            if (!auth) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_HARVESTER),
                        "Publisher Harvester Mongo auth failed. The provided credentials do not match. Exiting.");
                System.exit(-1);
            }
        }

        final Datastore dataStore = new Morphia().createDatastore(mongo, mongoConfig.getdBName());
        webResourceMetaInfoDao = new WebResourceMetaInfoDaoImpl(dataStore);
    }

    public void writeMetaInfos (Collection<HarvesterDocument> documents) {
        if (null == documents || documents.isEmpty()) {
            return ;
        }

        final List<WebResourceMetaInfo> webResourceMetaInfos = new ArrayList<>();

        for (final HarvesterDocument document: documents) {
            webResourceMetaInfos.add(
                  new WebResourceMetaInfo(document.getSourceDocumentReferenceMetaInfo().getId(),
                                          document.getSourceDocumentReferenceMetaInfo().getImageMetaInfo(),
                                          document.getSourceDocumentReferenceMetaInfo().getAudioMetaInfo(),
                                          document.getSourceDocumentReferenceMetaInfo().getVideoMetaInfo(),
                                          document.getSourceDocumentReferenceMetaInfo().getTextMetaInfo()
                                         )
            );
        }

        webResourceMetaInfoDao.createOrModify(webResourceMetaInfos, WriteConcern.ACKNOWLEDGED);
    }
}
