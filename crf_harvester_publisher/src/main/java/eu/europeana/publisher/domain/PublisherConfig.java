package eu.europeana.publisher.domain;

import eu.europeana.harvester.domain.MongoConfig;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

public class PublisherConfig {
    private final GraphiteReporterConfig graphiteConfig;

    /**
     * The timestamp which indicates the starting time of the publisher.
     * All metadata harvested after this timestamp will be published to MongoDB and Solr.
     */
    private final DateTime startTimestamp;

    private final String startTimestampFile;

    private final String stopGracefullyFile;

    /**
     * Batch of documents to update.
     */
    private final int batch;

    private final int delayInSecondsForRemainingRecordsStatistics;


    private final List<DBTargetConfig> targetMongoConfigs;
    private final Long sleepSecondsAfterEmptyBatch;

    private final MongoConfig sourceMongoConfig;

    public PublisherConfig (MongoConfig sourceMongoConfig, List<DBTargetConfig> targetMongoConfigs,
                            GraphiteReporterConfig graphiteConfig, DateTime startTimestamp, String startTimestampFile,String stopGracefullyFile,
                            Long sleepSecondsAfterEmptyBatch, int batch, int delayInSecondsForRemainingRecordsStatistics) {
        this.sourceMongoConfig = sourceMongoConfig;
        this.targetMongoConfigs = targetMongoConfigs;
        this.graphiteConfig = graphiteConfig;
        this.startTimestamp = startTimestamp;
        this.startTimestampFile = startTimestampFile;
        this.stopGracefullyFile = stopGracefullyFile;
        this.sleepSecondsAfterEmptyBatch = sleepSecondsAfterEmptyBatch;
        this.batch = batch;
        this.delayInSecondsForRemainingRecordsStatistics = delayInSecondsForRemainingRecordsStatistics;
    }

    public MongoConfig getSourceMongoConfig() {return sourceMongoConfig;}

    public List<DBTargetConfig> getTargetDBConfig () {
        return new ArrayList<>(targetMongoConfigs);
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

    public String getStopGracefullyFile() {
        return stopGracefullyFile;
    }

    public Integer getBatch () {
        return batch;
    }

    public Long getSleepSecondsAfterEmptyBatch () {
        return sleepSecondsAfterEmptyBatch;
    }

    public int getDelayInSecondsForRemainingRecordsStatistics () {
        return delayInSecondsForRemainingRecordsStatistics;
    }
}
