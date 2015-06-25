package eu.europeana.publisher.domain;

import eu.europeana.harvester.domain.MongoConfig;
import org.joda.time.DateTime;

import java.util.List;

public class PublisherConfig {
    private final GraphiteReporterConfig graphiteConfig;

    /**
     * The timestamp which indicates the starting time of the publisher.
     * All metadata harvested after this timestamp will be published to MongoDB and Solr.
     */
    private final DateTime startTimestamp;

    private final String startTimestampFile;

    /**
     * Batch of documents to update.
     */
    private final Integer batch;


    private final List<MongoConfig> targetMongoConfigs;
    private final DateTime sleepSecondsAfterEmptyBatch;
    private final List<String> solrUrls;

    private final MongoConfig sourceMongoConfig;

    public PublisherConfig (MongoConfig sourceMongoConfig, List<MongoConfig> targetMongoConfigs,
                            GraphiteReporterConfig graphiteConfig, DateTime startTimestamp, String startTimestampFile,
                            DateTime sleepSecondsAfterEmptyBatch, List<String> solrURLs, Integer batch) {
        this.sourceMongoConfig = sourceMongoConfig;
        this.targetMongoConfigs = targetMongoConfigs;
        this.graphiteConfig = graphiteConfig;
        this.startTimestamp = startTimestamp;
        this.startTimestampFile = startTimestampFile;
        this.sleepSecondsAfterEmptyBatch = sleepSecondsAfterEmptyBatch;
        this.solrUrls = solrURLs;
        this.batch = batch;
    }

    public MongoConfig getSourceMongoConfig () {
        return sourceMongoConfig;
    }

    public List<MongoConfig> getTargetMongoConfig () {
        return targetMongoConfigs;
    }

    public GraphiteReporterConfig getGraphiteConfig () {
        return graphiteConfig;
    }

    public DateTime getStartTimestamp () {
        return startTimestamp;
    }

    public String getStartTimestampFile () {
        return startTimestampFile;
    }

    public List<String> getSolrURL () {
        return solrUrls;
    }

    public Integer getBatch () {
        return batch;
    }

    public DateTime getSleepSecondsAfterEmptyBatch () {
        return sleepSecondsAfterEmptyBatch;
    }
}
