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

    public DefaultLimits(Long defaultBandwidthLimitReadInBytesPerSec, Long defaultMaxConcurrentConnectionsLimit,
                         Integer connectionTimeoutInMillis) {
        this.defaultBandwidthLimitReadInBytesPerSec = defaultBandwidthLimitReadInBytesPerSec;
        this.defaultMaxConcurrentConnectionsLimit = defaultMaxConcurrentConnectionsLimit;
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
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

}
