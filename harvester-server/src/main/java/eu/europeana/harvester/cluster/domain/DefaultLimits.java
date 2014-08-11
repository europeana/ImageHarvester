package eu.europeana.harvester.cluster.domain;

import java.io.Serializable;

public class DefaultLimits implements Serializable {

    /**
     * The default limit on download speed if it has not been provided.
     */
    private final Long defaultBandwidthLimitReadInBytesPerSec;

    /**
     * The default limit on concurrent number of downloads if it has not been provided.
     */
    private final Long defaultMaxConcurrentConnectionsLimit;

    /**
     * An int that specifies the connect timeout value in milliseconds.
     */
    private final Integer connectionTimeoutInMillis;

    /**
     * The number of redirects after which we stop the task.
     */
    private final Integer maxNrOfRedirects;

    public DefaultLimits(final Long defaultBandwidthLimitReadInBytesPerSec,
                         final Long defaultMaxConcurrentConnectionsLimit,
                         final Integer connectionTimeoutInMillis,
                         final Integer maxNrOfRedirects) {
        this.defaultBandwidthLimitReadInBytesPerSec = defaultBandwidthLimitReadInBytesPerSec;
        this.defaultMaxConcurrentConnectionsLimit = defaultMaxConcurrentConnectionsLimit;
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
        this.maxNrOfRedirects = maxNrOfRedirects;
    }

    public Long getDefaultBandwidthLimitReadInBytesPerSec() {
        return defaultBandwidthLimitReadInBytesPerSec;
    }

    public Long getDefaultMaxConcurrentConnectionsLimit() {
        return defaultMaxConcurrentConnectionsLimit;
    }

    public Integer getConnectionTimeoutInMillis() {
        return connectionTimeoutInMillis;
    }

    public Integer getMaxNrOfRedirects() {
        return maxNrOfRedirects;
    }
}
