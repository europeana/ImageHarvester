package eu.europeana.publisher;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.publisher.domain.GraphiteReporterConfig;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Publisher {
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private final Config config;

    public Publisher(final Config config) {
        if (null == config) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.config = config;
    }

    private DateTime readStartTimestamp() throws IOException {
        DateTime startTimestamp = null;
        try {
            startTimestamp = DateTime.parse(config.getString("criteria.startTimestamp"));
        }
        catch (ConfigException.Null e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                      "Start timestamp is null. Defaulting to 1 Jan 1970..");
        }


        String startTimestampFile;
        try {
            startTimestampFile = config.getString("criteria.startTimestampFile");
            if (null != startTimestampFile) {
                String file = FileUtils.readFileToString(new File(startTimestampFile), Charset.forName("UTF-8").name());
                if (StringUtils.isEmpty(file)) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                              "Start timestamp file is null. Defaulting to 1 Jan 1970..");
                }
                else {
                    startTimestamp = DateTime.parse(file.trim());
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                              "Start timestamp {} loaded from file.",startTimestamp);
                }
            }
            else {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                          "Start timestamp is null. Defaulting to 1 Jan 1970..");
            }
        }
        catch (ConfigException.Null e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                      "Start timestamp is null. Defaulting to 1 Jan 1970..");
        }
        catch (FileNotFoundException e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                      "Start timestamp file does not exist. Defaulting to 1 Jan 1970..");
        }

        return startTimestamp;
    }

    public void start() throws IOException, SolrServerException {
        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer = config.getString("metrics.graphiteServer");
        final Integer graphitePort = config.getInt("metrics.graphitePort");

        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(graphiteServer, graphiteMasterId, graphitePort);

        final MongoConfig sourceMongoConfig = MongoConfig.valueOf(config.getConfig("sourceMongoConfig"));
        final List<MongoConfig> targetMongoConfigs = new LinkedList<>();
        final List<String> solrUrls = new ArrayList<>();
        for (final Config target: config.getConfigList("targets")) {
            targetMongoConfigs.add(MongoConfig.valueOf(target.getConfig("mongo")));
            solrUrls.add(config.getString("solrUrl"));
        }

        final Integer batch = config.getInt("config.batch");

        final DateTime startTimestamp = readStartTimestamp();
        final String startTimestampFilename = config.getString("criteria.startTimestampFile");

        DateTime sleepSecondsAfterEmptyBatch = null;

        try {
           sleepSecondsAfterEmptyBatch = DateTime.parse(config.getString("criteria.sleepSecondsAfterEmptyBatch"));
        }
        catch (IllegalArgumentException e) {
           LOG.error("Error reading sleepSecondsAfterEmptyBatch", e);
        }

        final PublisherConfig publisherConfig = new PublisherConfig(sourceMongoConfig, targetMongoConfigs,
                                                                    graphiteReporterConfig,
                                                                    startTimestamp, startTimestampFilename,
                                                                    sleepSecondsAfterEmptyBatch,
                                                                    solrUrls, batch
                                                                   );




        final PublisherManager publisherManager = new PublisherManager(publisherConfig);
        publisherManager.start();
    }

    public void stop() throws IOException, SolrServerException {
        System.exit(0);
    }
}