package eu.europeana.crfmigration.domain;

import eu.europeana.harvester.domain.MongoConfig;
import org.joda.time.DateTime;

public class MigratorConfig {

    private final MongoConfig sourceMongoConfig;
    private final MongoConfig targetMongoConfig;
    private final GraphiteReporterConfig graphiteReporterConfig;
    private final int batch;
    public MigratorConfig(MongoConfig sourceMongoConfig, MongoConfig targetMongoConfig, GraphiteReporterConfig graphiteReporterConfig, int batch) {
        this.sourceMongoConfig = sourceMongoConfig;
        this.targetMongoConfig = targetMongoConfig;
        this.graphiteReporterConfig = graphiteReporterConfig;
        this.batch = batch;
    }

    public MongoConfig getSourceMongoConfig() {
        return sourceMongoConfig;
    }

    public MongoConfig getTargetMongoConfig() {
        return targetMongoConfig;
    }

    public GraphiteReporterConfig getGraphiteReporterConfig() {
        return graphiteReporterConfig;
    }

    public int getBatch() {
        return batch;
    }

    public MigratorConfig withBatchSize(int newBatch) {
        return new MigratorConfig(sourceMongoConfig,targetMongoConfig,graphiteReporterConfig,newBatch);
    }

}
