package eu.europeana.publisher;

import com.typesafe.config.Config;
import eu.europeana.publisher.domain.GraphiteReporterConfig;
import eu.europeana.publisher.domain.MongoConfig;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherManager;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Publisher {

    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private final DateTime startTimestamp;
    private final String startTimestampFile;
    private final Config config;

    public Publisher(final DateTime startTimestamp, final String startTimestampFile, final Config config) {
        this.startTimestamp = startTimestamp;
        this.startTimestampFile = startTimestampFile;
        this.config = config;
    }

    public void start() throws IOException, SolrServerException {

        final String solrURL = config.getString("solr.url");
        final Integer batch = config.getInt("config.batch");

        final List<? extends Config> sourceMongoConfigList = config.getConfigList("sourceMongo");
        final List<? extends Config> targetMongoConfigList = config.getConfigList("targetMongo");

        if (1 != targetMongoConfigList.size()) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                    "Target mongo configuration size is != 1. Currently the publisher does not support multiple targets. Exiting.");
            System.exit(-1);
        }

        if (1 != sourceMongoConfigList.size()) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                    "Source mongo configuration size is != 1. Currently the publisher does not support multiple sources. Exiting.");
            System.exit(-1);
        }

        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer = config.getString("metrics.graphiteServer");
        final Integer graphitePort = config.getInt("metrics.graphitePort");

        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(graphiteServer, graphiteMasterId, graphitePort);

        final Iterator<? extends Config> sourceMongoIter = sourceMongoConfigList.iterator();
        final Iterator<? extends Config> targetMongoIter = targetMongoConfigList.iterator();

        final MongoConfig sourceConfig = new MongoConfig(sourceMongoIter.next());
        final MongoConfig targetConfig = new MongoConfig(targetMongoIter.next());


        final PublisherConfig publisherConfig = new PublisherConfig(sourceConfig, targetConfig,
                graphiteReporterConfig, startTimestamp,
                startTimestampFile, solrURL, batch

        );
        final PublisherManager publisherManager = new PublisherManager(publisherConfig);
        publisherManager.start();
    }

    public void stop() throws IOException, SolrServerException {
        System.exit(0);
    }
}