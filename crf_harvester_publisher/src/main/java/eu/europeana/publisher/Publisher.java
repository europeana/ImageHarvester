package eu.europeana.publisher;

import com.typesafe.config.Config;
import eu.europeana.publisher.domain.GraphiteReporterConfig;
import eu.europeana.publisher.domain.MongoConfig;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logic.PublisherManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Publisher {

    private static Logger LOG = LogManager.getLogger(Publisher.class.getName());

    private final DateTime startTimestamp;
    private final String startTimestampFile;
    private final Config config;

    public Publisher(final DateTime startTimestamp, final String startTimestampFile, final Config config) {
        this.startTimestamp = startTimestamp;
        this.startTimestampFile = startTimestampFile;
        this.config = config;
    }

    public void start() throws IOException, SolrServerException {
        LOG.info("Start publishing ...");


        final String solrURL = config.getString("solr.url");
        final Integer batch = config.getInt("config.batch");

        final List<? extends Config> sourceMongoConfigList = config.getConfigList("sourceMongo");
        final List<? extends Config> targetMongoConfigList = config.getConfigList("targetMongo");

        if (sourceMongoConfigList.size() != targetMongoConfigList.size()) {
            LOG.error("sourceMongo configuration and targetMongo configuration from config file have different sizes!");
            System.exit(-1);
        }

        if (0 == sourceMongoConfigList.size()) {
            LOG.error("Empty source/target Mongo configurations from cofig file");
            System.exit(-1);
        }


        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(config);

        final Iterator<? extends Config> sourceMongoIter = sourceMongoConfigList.iterator();
        final Iterator<? extends Config> targetMongoIter = targetMongoConfigList.iterator();
        final List<Thread> threads = new ArrayList<>();

        while (sourceMongoIter.hasNext() && targetMongoIter.hasNext()) {
            final MongoConfig sourceMongoConfig = new MongoConfig(sourceMongoIter.next());
            final MongoConfig targetMongoConfig = new MongoConfig(targetMongoIter.next());

            final PublisherConfig publisherConfig = new PublisherConfig(
                    sourceMongoConfig, targetMongoConfig,
                    startTimestamp, startTimestampFile, solrURL, batch,
                    graphiteReporterConfig
            );

            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    final PublisherManager publisherManager;
                    try {
                        publisherManager = new PublisherManager(publisherConfig);
                        publisherManager.start();
                    } catch (Exception e) {
                        LOG.error("Exception was thrown", e);
                    }
                }
            }));
        }

        for (final Thread thread : threads) thread.start();

        for (final Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                LOG.error("Join failed for thread " + thread.getId(), e);
            }
        }
    }

    public void stop() throws IOException, SolrServerException {
        System.exit(0);
    }
}