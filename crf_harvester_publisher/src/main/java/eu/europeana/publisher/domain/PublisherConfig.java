package eu.europeana.publisher.domain;

import org.joda.time.DateTime;

public class PublisherConfig {
    private final String graphiteMasterId;

    private final String graphiteServer;

    private final Integer graphitePort;

    /**
     * Source MongoDB host.
     */
    private final String sourceHost;

    /**
     * Source MongoDB port.
     */
    private final Integer sourcePort;

    /**
     * The source DB name where the thumbnails will be stored.
     */
    private final String sourceDBName;

    /**
     * The db username if it's needed.
     */
    private final String sourceDBUsername;

    /**
     * The db password corresponding to the given username.
     */
    private final String sourceDBPassword;

    /**
     * Target MongoDB host.
     */
    private final String targetHost;

    /**
     * Target MongoDB port.
     */
    private final Integer targetPort;

    /**
     * The target DB name where the thumbnails will be stored.
     */
    private final String targetDBName;

    /**
     * The db username if it's needed.
     */
    private final String targetDBUsername;

    /**
     * The db password corresponding to the given username.
     */
    private final String targetDBPassword;

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

    public PublisherConfig( final String sourceHost, final Integer sourcePort, final String sourceDBName,
                           final String sourceDBUsername, final String sourceDBPassword, final String targetHost,
                           final Integer targetPort, final String targetDBName, final String targetDBUsername,
                           final String targetDBPassword, final DateTime startTimestamp, String startTimestampFile,
                           final String solrURL, final Integer batch,
                           final String graphiteMasterId, final String graphiteServer, final Integer graphitePort) {
        this.graphiteMasterId = graphiteMasterId;
        this.graphiteServer = graphiteServer;
        this.graphitePort = graphitePort;
        this.sourceHost = sourceHost;
        this.sourcePort = sourcePort;
        this.sourceDBName = sourceDBName;
        this.sourceDBUsername = sourceDBUsername;
        this.sourceDBPassword = sourceDBPassword;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.targetDBName = targetDBName;
        this.targetDBUsername = targetDBUsername;
        this.targetDBPassword = targetDBPassword;
        this.startTimestamp = startTimestamp;
        this.startTimestampFile = startTimestampFile;
        this.solrURL = solrURL;
        this.batch = batch;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public Integer getSourcePort() {
        return sourcePort;
    }

    public String getSourceDBName() {
        return sourceDBName;
    }

    public String getSourceDBUsername() {
        return sourceDBUsername;
    }

    public String getSourceDBPassword() {
        return sourceDBPassword;
    }

    public String getTargetHost() {
        return targetHost;
    }

    public Integer getTargetPort() {
        return targetPort;
    }

    public String getTargetDBName() {
        return targetDBName;
    }

    public String getTargetDBUsername() {
        return targetDBUsername;
    }

    public String getTargetDBPassword() {
        return targetDBPassword;
    }

    public DateTime getStartTimestamp() {
        return startTimestamp;
    }

    public String getStartTimestampFile() {
        return startTimestampFile;
    }

    public String getSolrURL() {
        return solrURL;
    }

    public Integer getBatch() {
        return batch;
    }

    @Override
    public String toString() {
        final StringBuffer buffer = new StringBuffer();

        buffer.append("--------------------------------------------------------------\n");
        buffer.append("sourceHost: " + sourceHost);
        buffer.append("sourcePort: " + sourcePort);
        buffer.append("sourceDBName: " + sourceDBName);
        buffer.append("sourceDBUsername: " + sourceDBUsername);
        buffer.append("sourceDBPassword: " + sourceDBPassword);
        buffer.append("targetHost: " + targetHost);
        buffer.append("targetPort: " + targetPort);
        buffer.append("targetDBName: " + targetDBName);
        buffer.append("targetDBUsername: " + targetDBUsername);
        buffer.append("targetDBPassword: " + targetDBPassword);
        buffer.append("startTimestamp: " + startTimestamp);
        buffer.append("startTimestampFile: " + startTimestampFile);
        buffer.append("--------------------------------------------------------------\n");

        return buffer.toString();
    }


    public String getGraphiteMasterId() {
        return graphiteMasterId;
    }

    public String getGraphiteServer() {
        return graphiteServer;
    }

    public Integer getGraphitePort() {
        return graphitePort;
    }
}
