package eu.europeana.publisher.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.WebResourceMetaInfoDAO;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDAOImpl;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import eu.europeana.publisher.domain.MongoConfig;
import eu.europeana.publisher.domain.RetrievedDocument;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherHarvesterDao {
    private static final Logger LOG = LogManager.getLogger(PublisherEuropeanaDao.class.getName());

    private WebResourceMetaInfoDAO webResourceMetaInfoDAO;

    public PublisherHarvesterDao (MongoConfig mongoConfig) throws UnknownHostException {

        if (null == mongoConfig ) {
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

        final Datastore dataStore = new Morphia().createDatastore(mongo, mongoConfig.getdBName());
        webResourceMetaInfoDAO = new WebResourceMetaInfoDAOImpl(dataStore);
    }

    public void writeMetaInfos (Collection<RetrievedDocument> documents) {
        if (null == documents || documents.isEmpty()) {
            return ;
        }

        final List<WebResourceMetaInfo> webResourceMetaInfos = new ArrayList<>();

        for (final RetrievedDocument document: documents) {
            webResourceMetaInfos.add(
                  new WebResourceMetaInfo(document.getSourceDocumentReferenceMetaInfo().getId(),
                                          document.getSourceDocumentReferenceMetaInfo().getImageMetaInfo(),
                                          document.getSourceDocumentReferenceMetaInfo().getAudioMetaInfo(),
                                          document.getSourceDocumentReferenceMetaInfo().getVideoMetaInfo(),
                                          document.getSourceDocumentReferenceMetaInfo().getTextMetaInfo()
                                         )
            );
        }

        webResourceMetaInfoDAO.create(webResourceMetaInfos, WriteConcern.ACKNOWLEDGED);
    }
}
