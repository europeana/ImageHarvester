package eu.europeana.harvester.domain;


import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

import java.util.UUID;

/**
 * Describes the link check limits. Fow now these are global limits that apply to all link checking in all collections.
 */
public class LinkCheckLimits {

    @Id
    @Property("id")
    private final String id;

    /**
     * The bandwidth limit usage for write (ie. sending). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitWriteInBytesPerSec;

    /**
     * The bandwidth limit usage for read (ie. receiving). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitReadInBytesPerSec;

    /**
     * The time threshold after which the retrieval is terminated. 0 means no limit.
     */
    private final Long terminationThresholdTimeLimit;

    /**
     * The content size threshold after which the retrieval is terminated. 0 means no limit.
     */
    private final Long terminationThresholdSizeLimitInBytes;

    public LinkCheckLimits() {
        this.id = null;
        this.bandwidthLimitWriteInBytesPerSec = null;
        this.bandwidthLimitReadInBytesPerSec = null;
        this.terminationThresholdTimeLimit = null;
        this.terminationThresholdSizeLimitInBytes = null;
    }

    public LinkCheckLimits(final Long bandwidthLimitWriteInBytesPerSec, final Long bandwidthLimitReadInBytesPerSec,
                           final Long terminationThresholdTimeLimit, final Long terminationThresholdSizeLimitInBytes) {
        this.id = UUID.randomUUID().toString();
        this.bandwidthLimitWriteInBytesPerSec = bandwidthLimitWriteInBytesPerSec;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.terminationThresholdTimeLimit = terminationThresholdTimeLimit;
        this.terminationThresholdSizeLimitInBytes = terminationThresholdSizeLimitInBytes;
    }

    public LinkCheckLimits(final String id, final Long bandwidthLimitWriteInBytesPerSec,
                           final Long bandwidthLimitReadInBytesPerSec, final Long terminationThresholdTimeLimit,
                           final Long terminationThresholdSizeLimitInBytes) {
        this.id = id;
        this.bandwidthLimitWriteInBytesPerSec = bandwidthLimitWriteInBytesPerSec;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.terminationThresholdTimeLimit = terminationThresholdTimeLimit;
        this.terminationThresholdSizeLimitInBytes = terminationThresholdSizeLimitInBytes;
    }

    public String getId() {
        return id;
    }

    public Long getBandwidthLimitWriteInBytesPerSec() {
        return bandwidthLimitWriteInBytesPerSec;
    }

    public Long getBandwidthLimitReadInBytesPerSec() {
        return bandwidthLimitReadInBytesPerSec;
    }

    public Long getTerminationThresholdTimeLimit() {
        return terminationThresholdTimeLimit;
    }

    public Long getTerminationThresholdSizeLimitInBytes() {
        return terminationThresholdSizeLimitInBytes;
    }
}
