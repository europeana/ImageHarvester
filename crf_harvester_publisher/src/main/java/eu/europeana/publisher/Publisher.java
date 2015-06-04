package eu.europeana.publisher;

import com.typesafe.config.Config;
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

        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer = config.getString("metrics.graphiteServer");
        final Integer graphitePort = config.getInt("metrics.graphitePort");

        final Iterator<? extends Config> sourceMongoIter = sourceMongoConfigList.iterator();
        final Iterator<? extends Config> targetMongoIter = targetMongoConfigList.iterator();
        final List<Thread> threads = new ArrayList<>();

        while (sourceMongoIter.hasNext() && targetMongoIter.hasNext()) {
            final Config sourceMongoConfig = sourceMongoIter.next();
            final Config targetMongoConfig = targetMongoIter.next();

            final String sourceHost = sourceMongoConfig.getString("host");
            final Integer sourcePort = sourceMongoConfig.getInt("port");
            final String sourceDBName = sourceMongoConfig.getString("dbName");
            final String sourceDBUsername = sourceMongoConfig.getString("username");
            final String sourceDBPassword = sourceMongoConfig.getString("password");

            final String targetHost = targetMongoConfig.getString("host");
            final Integer targetPort = targetMongoConfig.getInt("port");
            final String targetDBName = targetMongoConfig.getString("dbName");
            final String targetDBUsername = targetMongoConfig.getString("username");
            final String targetDBPassword = targetMongoConfig.getString("password");

            final PublisherConfig publisherConfig = new PublisherConfig(sourceHost, sourcePort, sourceDBName,
                    sourceDBUsername, sourceDBPassword, targetHost, targetPort, targetDBName, targetDBUsername,
                    targetDBPassword, startTimestamp, startTimestampFile, solrURL, batch,
                    graphiteMasterId, graphiteServer, graphitePort);

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