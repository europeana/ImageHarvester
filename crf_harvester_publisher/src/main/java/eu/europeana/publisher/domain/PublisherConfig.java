package eu.europeana.publisher.domain;

import org.joda.time.DateTime;

public class PublisherConfig {
    private final MongoConfig sourceMongoConfig;
    private final MongoConfig targetMongoConfig;
    private final GraphiteReporterConfig graphiteConfig;

    /**
     * The timestamp which indicates the starting time of the publisher.
     * All metadata harvested after this timestamp will be published to MongoDB and Solr.
     */
    private final DateTime startTimestamp;

    private final String startTimestampFile;

    /**
     * The URL of the Solr instance.
     * e.g.: http://IP:Port/solr
     */
    private final String solrURL;

    /**
     * Batch of documents to update.
     */
    private final Integer batch;

    public PublisherConfig (MongoConfig sourceMongoConfig, MongoConfig targetMongoConfig,
                            GraphiteReporterConfig graphiteConfig, DateTime startTimestamp,
                            String startTimestampFile, String solrURL, Integer batch) {
        this.sourceMongoConfig = sourceMongoConfig;
        this.targetMongoConfig = targetMongoConfig;
        this.graphiteConfig = graphiteConfig;
        this.startTimestamp = startTimestamp;
        this.startTimestampFile = startTimestampFile;
        this.solrURL = solrURL;
        this.batch = batch;
    }

    public MongoConfig getSourceMongoConfig () {
        return sourceMongoConfig;
    }

    public MongoConfig getTargetMongoConfig () {
        return targetMongoConfig;
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

    public String getSolrURL () {
        return solrURL;
    }

    public Integer getBatch () {
        return batch;
    }
}
