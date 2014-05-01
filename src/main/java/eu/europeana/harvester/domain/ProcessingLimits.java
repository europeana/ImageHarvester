package eu.europeana.harvester.domain;

/**
 * Cluster wide processing limits per collection.
 */
public class ProcessingLimits {

    private final Long id;

    private final Long collectionId;

    /**
     * The bandwidth limit usage for read (ie. receiving). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitReadInBytesPerSec;

    /**
     * The maximum number of concurrent connections allowed per collection.
     */
    private final Long maxConcurrentConnectionsLimit;

    public ProcessingLimits(Long id, Long collectionId, Long bandwidthLimitReadInBytesPerSec, Long maxConcurrentConnectionsLimit) {
        this.id = id;
        this.collectionId = collectionId;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.maxConcurrentConnectionsLimit = maxConcurrentConnectionsLimit;
    }

    public Long getId() {
        return id;
    }

    public Long getCollectionId() {
        return collectionId;
    }

    public Long getBandwidthLimitReadInBytesPerSec() {
        return bandwidthLimitReadInBytesPerSec;
    }

    public Long getMaxConcurrentConnectionsLimit() {
        return maxConcurrentConnectionsLimit;
    }
}
