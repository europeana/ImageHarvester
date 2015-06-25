package eu.europeana.publisher.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import eu.europeana.publisher.domain.HarvesterDocument;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherHarvesterDao {
    private WebResourceMetaInfoDao webResourceMetaInfoDao;

    public PublisherHarvesterDao (MongoConfig mongoConfig) throws UnknownHostException {

        if (null == mongoConfig ) {
            throw new IllegalArgumentException ("mongoConfig cannot be null");
        }

        final Datastore dataStore = new Morphia().createDatastore(mongoConfig.connectToMongo(), mongoConfig.getdBName());
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
