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


    private final List<DBTargetConfig> targetMongoConfigs;
    private final Long sleepSecondsAfterEmptyBatch;

    private final MongoConfig sourceMongoConfig;

    public PublisherConfig (MongoConfig sourceMongoConfig, List<DBTargetConfig> targetMongoConfigs,
                            GraphiteReporterConfig graphiteConfig, DateTime startTimestamp, String startTimestampFile,
                            Long sleepSecondsAfterEmptyBatch, Integer batch) {
        this.sourceMongoConfig = sourceMongoConfig;
        this.targetMongoConfigs = targetMongoConfigs;
        this.graphiteConfig = graphiteConfig;
        this.startTimestamp = startTimestamp;
        this.startTimestampFile = startTimestampFile;
        this.sleepSecondsAfterEmptyBatch = sleepSecondsAfterEmptyBatch;
        this.batch = batch;
    }

    public MongoConfig getSourceMongoConfig() {return sourceMongoConfig;}

    public List<DBTargetConfig> getTargetDBConfig () {
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

    public Integer getBatch () {
        return batch;
    }

    public Long getSleepSecondsAfterEmptyBatch () {
        return sleepSecondsAfterEmptyBatch;
    }
}
