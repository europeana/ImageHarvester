package eu.europeana.publisher.dao;

import com.drew.lang.annotations.NotNull;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import eu.europeana.harvester.db.MorphiaDataStore;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;
import eu.europeana.publisher.domain.MongoConfig;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.transform.Source;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by salexandru on 03.06.2015.
 */
public class PublisherEuropeanaDAO {
    private static final Logger LOG = LogManager.getLogger(PublisherEuropeanaDAO.class.getName());

    private PublisherMetrics metrics;
    private DB mongoDB;

    public PublisherEuropeanaDAO (@NotNull MongoConfig mongoConfig, @NotNull PublisherMetrics metrics) throws
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
    }

    public List<SourceDocumentProcessingStatistics> retrieveSourceDocumentProcessingStatistics(final DBCursor cursor, final int batchSize) {
        final List<SourceDocumentProcessingStatistics> sourceDocumentProcessingStatisticsList = new ArrayList<>();

        for (final DBObject dbObject: cursor.batchSize(batchSize)) {

        }


        return sourceDocumentProcessingStatisticsList;
    }
}
