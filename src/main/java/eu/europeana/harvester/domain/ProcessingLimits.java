package eu.europeana.harvester.domain;

import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;

import java.util.UUID;

/**
 * Cluster wide processing limits per collection.
 */
public class ProcessingLimits {

    @Id
    @Property("id")
    private final String id;

    private final Long collectionId;

    /**
     * The bandwidth limit usage for read (ie. receiving). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitReadInBytesPerSec;

    /**
     * The maximum number of concurrent connections allowed per collection.
     */
    private final Long maxConcurrentConnectionsLimit;

    public ProcessingLimits() {
        this.id = null;
        this.collectionId = null;
        this.bandwidthLimitReadInBytesPerSec = null;
        this.maxConcurrentConnectionsLimit = null;
    }

    public ProcessingLimits(Long collectionId, Long bandwidthLimitReadInBytesPerSec,
                            Long maxConcurrentConnectionsLimit) {
        this.id = UUID.randomUUID().toString();
        this.collectionId = collectionId;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.maxConcurrentConnectionsLimit = maxConcurrentConnectionsLimit;
    }

    public ProcessingLimits(String id, Long collectionId, Long bandwidthLimitReadInBytesPerSec,
                            Long maxConcurrentConnectionsLimit) {
        this.id = id;
        this.collectionId = collectionId;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.maxConcurrentConnectionsLimit = maxConcurrentConnectionsLimit;
    }

    public String getId() {
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
